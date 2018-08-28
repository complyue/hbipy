import asyncio
import inspect
import logging
from collections import deque
from typing import *

from .buflist import *
from .bytesbuf import *
from .context import run_in_context
from .sendctrl import SendCtrl

__all__ = [
    'AbstractHBIC',
]

logger = logging.getLogger(__name__)


def chain_future(from_fut, to_fut):
    assert asyncio.isfuture(from_fut)

    if to_fut.done():
        # target already cancelled etc.
        return

    def from_fut_cb(fut):

        if to_fut.done():
            # target already cancelled etc.
            return

        if fut.cancelled():
            to_fut.cancel()
        elif fut.exception() is not None:
            to_fut.set_exception(fut.exception())
        else:
            fr = fut.result()
            if asyncio.isfuture(fr):
                # continue the from_fut chain
                chain_future(fr, to_fut)
            else:
                # just resolved
                to_fut.set_result(fr)

    asyncio.ensure_future(from_fut).add_done_callback(from_fut_cb)


class AbstractHBIC:
    """
    Abstract HBI Connection

    """

    @staticmethod
    def cast_to_src_buffer(boc):
        if isinstance(boc, (bytes, bytearray)):
            # it's a bytearray
            return boc
        if not isinstance(boc, memoryview):
            try:
                boc = memoryview(boc)
            except TypeError:
                return None
        # it's a memoryview now
        if boc.nbytes == 0:  # if zero-length, replace with empty bytes
            # coz when a zero length ndarray is viewed, cast/send will raise while not needed at all
            return b''
        elif boc.itemsize != 1:
            return boc.cast('B')
        return boc

    @staticmethod
    def cast_to_tgt_buffer(boc):
        if isinstance(boc, bytes):
            raise TypeError('bytes can not be target buffer since readonly')
        if isinstance(boc, bytearray):
            # it's a bytearray
            return boc
        if not isinstance(boc, memoryview):
            try:
                boc = memoryview(boc)
            except TypeError:
                return None
        # it's a memoryview now
        if boc.readonly:
            raise TypeError('readonly memoryview can not be target buffer')
        if boc.nbytes == 0:  # if zero-length, replace with empty bytes
            # coz when a zero length ndarray is viewed, cast/send will raise while not needed at all
            return b''
        elif boc.itemsize != 1:
            return boc.cast('B')
        return boc

    __slots__ = (
        'context',
        'addr', 'net_opts',
        'send_only',
        '_disconnecting',

        '_loop',
        '_wire_fut', '_wire', '_wire_ctx', '_data_sink', '_conn_fut', '_disc_fut',

        'low_water_mark_send', 'high_water_mark_send', '_send_mutex',

        '_recv_buffer', 'app_queue_size',
        'low_water_mark_recv', 'high_water_mark_recv',
        '_landed_queue', '_recv_obj_waiters',

        '_corun_mutex', '_co_remote_ack',
        '_co_local_ack', '_co_local_done',
    )

    def __init__(
            self,
            context,
            addr=None, net_opts=None,
            *,
            send_only=False, app_queue_size: int = 500,
            low_water_mark_send=6 * 1024 * 1024, high_water_mark_send=20 * 1024 * 1024,
            low_water_mark_recv=6 * 1024 * 1024, high_water_mark_recv=20 * 1024 * 1024,
            loop=None,
            **kwargs
    ):
        super().__init__(**kwargs)
        context['hbi_peer'] = self
        self.context = context
        self.addr = addr
        self.net_opts = net_opts
        self.send_only = send_only
        self._disconnecting = False
        if loop is None:
            loop = asyncio.get_event_loop()
        self._loop = loop

        self._wire_fut = None
        self._wire = None
        self._wire_ctx = None
        self._data_sink = None
        self._conn_fut = None
        self._disc_fut = None

        self.low_water_mark_send = low_water_mark_send
        self.high_water_mark_send = high_water_mark_send
        self._send_mutex = SendCtrl(loop=loop)

        self._recv_buffer = None
        self.app_queue_size = int(app_queue_size)
        self.low_water_mark_recv = low_water_mark_recv
        self.high_water_mark_recv = high_water_mark_recv
        self._landed_queue = deque()
        self._recv_obj_waiters = deque()

        self._corun_mutex = asyncio.Lock(loop=loop)
        self._co_remote_ack = None
        self._co_local_ack = None
        self._co_local_done = None

    def __str__(self):
        return f'HBI:{self.net_info}'

    def __repr__(self):
        return repr(str(self))

    def _handle_wire_error(self, exc):
        import traceback
        traceback.print_exc()
        logger.fatal(f'HBI {self.net_info} wire error occurred, forcing disconnection.')
        err_stack = traceback.format_exc()

        self._cut_wire(exc, err_stack)

    def _handle_landing_error(self, exc):
        try:

            # if the HBI module declared an error handler, let it try handle the error first.
            # the handler returns True to indicate that this error should be tolerated i.e. ignored.
            modu_err_handler = self.context.get('hbi_handle_err', None)
            if modu_err_handler is not None:
                if True is modu_err_handler(exc):
                    # HBI module claimed successful handling of this error
                    return

            # if in corun mode with at least one waiter, leave the error to be thrown into the coro
            # running
            if self._corun_mutex.locked() and self._recv_obj_waiters:
                return

        except Exception:
            # error in error handling, will be reflected nextly
            pass

        import traceback
        traceback.print_exc()
        logger.fatal(f'HBI {self.net_info} landing error occurred, forcing disconnection.')
        err_stack = traceback.format_exc()
        self.disconnect(exc, err_stack, True)

    def _handle_peer_error(self, message, stack=None):
        try:
            modu_err_handler = self.context.get('hbi_handle_peer_err', None)
            if modu_err_handler is not None:
                if True is modu_err_handler(message, stack):
                    # HBI module claimed successful handling of this error
                    return

            if self._recv_obj_waiters:
                # propagate peer error to co-receivers
                exc = RuntimeError(f'HBI peer error: {message!s}')
                waiters = self._recv_obj_waiters
                self._recv_obj_waiters = deque()
                for waiter in waiters:
                    if waiter.done():
                        # may have been cancelled etc.
                        continue
                    waiter.set_exception(exc)

        except Exception:
            import traceback
            traceback.print_exc()
            logger.fatal(f'HBI {self.net_info} error occurred in handling peer error.')

        self.disconnect(f'Peer error: {message}', stack, False)

    @property
    def loop(self):
        return self._loop

    @property
    def connected(self):
        return self._wire is not None

    @property
    def net_info(self):
        _wire = self._wire
        if not _wire:
            return '<unwired>'
        return f'[{_wire}]'

    def run_until_disconnected(self, ensure_connected=True):
        if not self.connected:
            # disconnected atm

            if not ensure_connected:
                # nop in this case
                return

            # initiate connection now
            self.connect()

        if self._disc_fut is None:
            self._disc_fut = self._loop.create_future()
        self._loop.run_until_complete(self._disc_fut)

    def disconnect(self, err_reason=None, err_stack=None, try_send_peer_err=True):
        if self._disconnecting:
            logger.debug(f'Repeating disconnection request ignored. err_reason: {err_reason!s}')
            return
        self._disconnecting = True

        if err_reason is not None:
            if err_stack is None and isinstance(err_reason, BaseException):
                import traceback
                err_stack = traceback.format_exc()

            logger.error(rf'''
HBI disconnecting {self.net_info} due to error:
 ---
{err_reason}
 ---
{err_stack or '<no-stack>'}
 ===''', stack_info=True)

        disconn_cb = self.context.get('hbi_disconnecting', None)
        if disconn_cb is not None:
            maybe_coro = disconn_cb(err_reason)
            if inspect.isawaitable(maybe_coro):
                asyncio.ensure_future(maybe_coro)

        if self._loop.is_closed():
            logger.warning('HBI disconnection bypassed since loop already closed.')
            return

        # this can be called from any thread
        self._loop.call_soon_threadsafe(
            self._loop.create_task, self._disconnect(err_reason, err_stack, try_send_peer_err)
        )

    async def wait_disconnected(self):
        if not self.connected:
            return

        if self._disc_fut is None:
            self._disc_fut = self._loop.create_future()
        await self._disc_fut

    def _cut_wire(self, err_reason=None, err_stack=None):
        raise NotImplementedError('subclass should implement this')

    def _disconnect(self, err_reason=None, err_stack=None, try_send_peer_err=True):
        raise NotImplementedError('subclass should implement this as a coroutine')

    def _disconnected(self, exc=None):
        self._disconnecting = False

        if exc is not None:
            logger.warning(f'HBI connection unwired due to error: {exc}')

        # abort pending tasks
        self._recv_buffer = None
        if self._recv_obj_waiters:
            if not exc:
                exc = RuntimeError('HBI disconnected')
            waiters = self._recv_obj_waiters
            self._recv_obj_waiters = deque()
            for waiter in waiters:
                if waiter.done():
                    # may have been cancelled etc.
                    continue
                waiter.set_exception(exc)

        self._send_mutex.shutdown(exc)

        fut = self._disc_fut
        if fut is not None:
            self._disc_fut = None
            if not fut.done():
                fut.set_result(self)

        disconn_cb = self.context.get('hbi_disconnected', None)
        if disconn_cb is not None:
            maybe_coro = disconn_cb(exc)
            if inspect.isawaitable(maybe_coro):
                asyncio.ensure_future(maybe_coro)

    def _peer_eof(self):
        peer_done_cb = self.context.get('hbi_peer_done', None)
        if peer_done_cb is not None:
            return peer_done_cb()

    def _connect(self):
        raise NotImplementedError('subclass should implement this as a coroutine')

    def connect(self, addr=None, net_opts=None):
        self._disconnecting = False

        if self.connected:
            if (
                    addr is None or addr == self.addr
            ) and (
                    net_opts is None or net_opts == self.net_opts
            ):
                # already connected as expected
                return
            raise asyncio.InvalidStateError(
                'already connected to different addr, disconnect before connect to else where'
            )
        if addr is not None:
            self.addr = addr
        if net_opts is not None:
            self.net_opts = None
        if not self.addr:
            raise asyncio.InvalidStateError('attempt connecting without addr')

        # this can be called from any thread
        self._loop.call_soon_threadsafe(self._loop.create_task, self._connect())

    def _connected(self):

        self._recv_buffer = BufferList()
        self._send_mutex.startup()

        fut = self._conn_fut
        if fut is not None:
            self._conn_fut = None
            if not fut.done():
                fut.set_result(self)

        conn_cb = self.context.get('hbi_connected', None)
        if conn_cb is not None:
            maybe_coro = conn_cb()
            if inspect.isawaitable(maybe_coro):
                asyncio.ensure_future(maybe_coro)

    async def wait_connected(self):
        if self.connected:
            return

        self.connect()

        if self._conn_fut is None:
            self._conn_fut = self._loop.create_future()
        await self._conn_fut

    def run_until_connected(self):
        if self.connected:
            return

        return self._loop.run_until_complete(self.wait_connected())

    # implement HBIC as reusable async context manager for grace disconnection

    async def __aenter__(self):
        await self.wait_connected()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            err_reason = None
            err_stack = None
        else:
            import traceback
            err_reason = exc_val if isinstance(exc_val, BaseException) else exc_type(exc_val)
            err_stack = ''.join(traceback.format_exception(exc_type, exc_val, exc_tb))

        self.disconnect(err_reason, err_stack, try_send_peer_err=True)
        await self.wait_disconnected()

    @property
    def in_corun(self):
        """
        whether local peer is currently in corun mode

        """
        if self._co_remote_ack is not None:
            assert self._corun_mutex.locked(), '?!'
            return True
        else:
            assert not self._corun_mutex.locked(), '?!'
            return False

    def fire(self, code):
        """
        Fire a piece of plain code for remote execution and forget about it.

        """
        self._loop.call_soon_threadsafe(
            self.notif, code,
        )

    def fire_corun(self, code, bufs=None):
        """
        Fire a piece of corun code, optionally with binary data, for remote execution
        and forget about it.

        """
        self._loop.call_soon_threadsafe(
            self.notif_corun, code, bufs,
        )

    def notif(self, code):
        """
        Schedule a piece of plain code to be executed remotely for a notification.

        The actual sending will be postponed to next send opportunity, when locally no
        other corun conversation or notification is on the go.

        The returned task can be awaited or chained for further processing.

        Warning: if you await the returned task from a local corun conversaion,
        deadlock will occur because that task won't be startd until current
        conversaion ended.

        """

        async def notif_out_sending():
            async with self._send_mutex:
                await self._send_code(code)

        return self._loop.create_task(notif_out_sending())

    def notif_corun(self, code, bufs=None):
        """
        Schedule a piece of corun code to be executed remotely for a notification.

        The actual sending will be postponed to next send opportunity, when locally no
        other corun conversation or notification is on the go.

        The returned task can be awaited or chained for further processing.

        Warning: if you await the returned task from a local corun conversaion,
        deadlock will occur because that task won't be startd until current
        conversaion ended.

        """

        async def notif_corun_out_sending():
            async with self.co():
                await self._send_code(code, b'corun')
                if bufs is not None:
                    await self._send_data(bufs)

        return self._loop.create_task(notif_corun_out_sending())

    async def co_send_corun(self, code):
        """
        Send a piece of code to be executed remotely, but its direct result
        has no impact to peer conversation.
        While the execution can also generate any number of `co_send_code()`
        and/or `co_send_data()` calls at peer, for subsequent calls to
        `co_recv_obj()` and/or `co_recv_data()` from local corun
        conversation to receive them.

        Must in a corun conversation or RuntimeError will raise.

        """
        if self._co_remote_ack is not None:
            # in active corun conversation, remote may not have acked suspension
            # of other sendings, but that's okay for pipelined out sending from
            # local conversation
            local_co_id = id(self._co_remote_ack)
        else:
            # not in active corun conversation
            if self._co_local_ack is None:
                # nor in passive corun conversation
                raise RuntimeError('Not in corun conversation!')
            # in passive corun conversation
            # must wait until co_ack has been put on wire
            remote_co_id = await self._co_local_ack

        await self._send_code(code, b'corun')

    async def co_send_code(self, code):
        """
        Send a piece of code to be executed remotely, whose direct result will
        feed into a `co_recv_obj()` call waiting within peer conversation.
        While the execution can also generate any number of `co_send_code()`
        and/or `co_send_data()` calls at peer, for subsequent calls to
        `co_recv_obj()` and/or `co_recv_data()` from local corun
        conversation to receive them.

        Must in a corun conversation or RuntimeError will raise.

        """
        if self._co_remote_ack is not None:
            # in active corun conversation, remote may not have acked suspension
            # of other sendings, but that's okay for pipelined out sending from
            # local conversation
            local_co_id = id(self._co_remote_ack)
        else:
            # not in active corun conversation
            if self._co_local_ack is None:
                # nor in passive corun conversation
                raise RuntimeError('Not in corun conversation!')
            # in passive corun conversation
            # must wait until co_ack has been put on wire
            remote_co_id = await self._co_local_ack

        await self._send_code(code)

    async def co_send_data(self, bufs):
        """
        Send a bulk of binary data meant to be received by the remote corun
        conversation, the remote conversation must know the exact size of the
        data from previous communications, or the HBI wire will get corrupted.

        Must in a corun conversation or RuntimeError will raise.

        """
        if self._co_remote_ack is not None:
            # in active corun conversation, remote may not have acked suspension
            # of other sendings, but that's okay for pipelined out sending from
            # local conversation
            local_co_id = id(self._co_remote_ack)
        else:
            # not in active corun conversation
            if self._co_local_ack is None:
                # nor in passive corun conversation
                raise RuntimeError('Not in corun conversation!')
            # in passive corun conversation
            # must wait until co_ack has been put on wire
            remote_co_id = await self._co_local_ack

        await self._send_data(bufs)

    async def co_get(self, code):
        """
        Send a piece of code to be executed remotely, and receive its result
        right back.

        Must in a corun conversation or RuntimeError will raise.

        """
        if self._co_remote_ack is not None:
            # in active corun conversation, remote may not have acked suspension
            # of other sendings, but that's okay for pipelined out sending from
            # local conversation
            local_co_id = id(self._co_remote_ack)
        else:
            # not in active corun conversation
            if self._co_local_ack is None:
                # nor in passive corun conversation
                raise RuntimeError('Not in corun conversation!')
            # in passive corun conversation
            # must wait until co_ack has been put on wire
            remote_co_id = await self._co_local_ack

        await self._send_code(code, b'coget')
        result = await self.co_recv_obj()
        return result

    async def _send_code(self, code, wire_dir=None):
        if not self.connected:
            raise RuntimeError('HBI wire not connected')

        # convert wire_dir as early as possible, will save conversions in case
        # the code is of complex structure
        if wire_dir is None:
            wire_dir = b''
        elif not isinstance(wire_dir, (bytes, bytearray)):
            wire_dir = str(wire_dir).encode('utf-8')

        # use a generator function to pull code from hierarchy
        def pull_code(container):
            for mc in container:
                if inspect.isgenerator(mc):
                    yield from pull_code(mc)
                else:
                    yield mc

        if inspect.isgenerator(code):
            for c in pull_code(code):
                await self._send_text(c, wire_dir)
        else:
            await self._send_text(code, wire_dir)

    async def _send_data(self, bufs):
        assert bufs is not None

        # use a generator function to pull all buffers from hierarchy

        def pull_from(boc):
            b = self.cast_to_src_buffer(boc)  # this static method can be overridden by subclass
            if b is not None:
                yield b
                return
            for boc1 in boc:
                yield from pull_from(boc1)

        for buf in pull_from(bufs):
            max_chunk_size = self.high_water_mark_send
            remain_size = len(buf)
            send_from_idx = 0
            while remain_size > max_chunk_size:
                await self._send_buffer(buf[send_from_idx: send_from_idx + max_chunk_size])
                send_from_idx += max_chunk_size
                remain_size -= max_chunk_size
            if remain_size > 0:
                await self._send_buffer(buf[send_from_idx:])

    async def _send_text(self, code, wire_dir=b''):
        raise NotImplementedError

    async def _send_buffer(self, buf):
        raise NotImplementedError

    def _recv_water_pos(self):
        return self._recv_buffer.nbytes

    def _pause_recv(self):
        raise NotImplementedError

    def _resume_recv(self):
        raise NotImplementedError

    def _land_one(self) -> Optional[tuple]:
        """
        Interpretation of the return value from this function:
            tuple of 3 - (exception, coro_task, coroutine)
            tuple of 2 - (exception, value)
            tuple of 1 - (affair,) result of HBI protocol affair, NO value goes to the application layer
            None - no packet from wire landed
        """
        raise NotImplementedError

    async def _coget_helper(self, code):
        if self._co_remote_ack is not None:
            # in active corun conversation, remote may not have acked suspension
            # of other sendings, but that's okay for pipelined out sending from
            # local conversation
            local_co_id = id(self._co_remote_ack)
        else:
            # not in active corun conversation
            if self._co_local_ack is None:
                # nor in passive corun conversation
                raise RuntimeError('coget out of corun conversation!')
            # in passive corun conversation
            # wait until co_ack has been put on wire
            remote_co_id = await self._co_local_ack

        defs = {}
        try:

            # execute code for result
            maybe_coro = run_in_context(code, self.context, defs)
            if inspect.iscoroutine(maybe_coro):
                result = await maybe_coro
            else:
                result = maybe_coro

            # send result back
            await self._send_code(repr(result))

        except Exception as exc:
            logger.error(rf'''
HBI {self.net_info}, error co-getting for remote conversation {remote_co_id}:
--CODE--
{code!s}
--DEFS--
{defs!r}
--====--
''', exc_info=True)
            # try handle the error by hbic class
            self._handle_landing_error(exc)

    async def _corun_helper(self, code):
        if self._co_remote_ack is not None:
            # in active corun conversation, remote may not have acked suspension
            # of other sendings, but that's okay for pipelined out sending from
            # local conversation
            local_co_id = id(self._co_remote_ack)
        else:
            # not in active corun conversation
            if self._co_local_ack is None:
                # nor in passive corun conversation
                raise RuntimeError('coget out of corun conversation!')
            # in passive corun conversation
            # wait until co_ack has been put on wire
            remote_co_id = await self._co_local_ack

        defs = {}
        try:

            # execute corun code
            maybe_coro = run_in_context(code, self.context, defs)
            if inspect.iscoroutine(maybe_coro):
                await maybe_coro

        except Exception as exc:
            logger.error(rf'''
HBI {self.net_info}, error co-getting for remote conversation {remote_co_id}:
--CODE--
{code!s}
--DEFS--
{defs!r}
--====--
''', exc_info=True)
            # try handle the error by hbic class
            self._handle_landing_error(exc)

    async def _co_helper(self, remote_co_id):
        local_ack, local_done = self._co_local_ack, self._co_local_done
        assert local_ack is not None and not local_ack.done(), '?!'
        async with self._send_mutex:
            # prevent other sending since the co_ack sent to remote
            # until remote closed corun conversation by sending co_end,
            # which will trigger local_done, then this coro finish
            await self._send_text(repr(remote_co_id), b'co_ack')
            local_ack.set_result(remote_co_id)
            done_co_id = await local_done
            assert done_co_id == remote_co_id, '?!'

    def _land_(self, code, wire_dir) -> Optional[tuple]:
        # Semantic of return value from this function is same as _land_one()

        # process non-customizable wire directives first
        if 'co_begin' == wire_dir:

            if self._co_local_ack is not None:
                self.disconnect('Unclean co_begin!')
                return None,
            remote_co_id = eval(code)
            self._co_local_ack = self._loop.create_future()
            self._co_local_done = self._loop.create_future()
            # use a helper coro to hold the send mutex during remote corun
            # conversation, so no extra object/data except those requested by
            # that conversation can be sent to remote peer before it ends.
            self._loop.create_task(self._co_helper(remote_co_id))
            return None,

        elif 'co_end' == wire_dir:

            remote_co_id = eval(code)
            local_ack, local_done = self._co_local_ack, self._co_local_done
            if local_ack is None or local_done is None:
                self.disconnect('Uninitialized co_end!')
                return None,
            assert not local_done.done(), 'done already ?! extra co_end ?!'

            def local_co_end(fut):
                assert fut is local_ack
                if local_ack.result() != remote_co_id:
                    self.disconnect('Mismatch co_end!')
                else:
                    local_done.set_result(remote_co_id)
                    self._co_local_ack, self._co_local_done = None, None
                    if self._landed_queue:
                        logger.warning(
                            f'Non-empty app queue ({len(self._landed_queue)} objects)'
                            f' at end of passive conversation.'
                        )
                        self._landed_queue.clear()

            local_ack.add_done_callback(local_co_end)
            # use a done callback so racing condition is no problem
            return None,

        elif 'co_ack' == wire_dir:

            remote_ack = self._co_remote_ack
            if remote_ack is None or remote_ack.done():
                self.disconnect('Unexpected co_ack!')
                return None,
            received_co_id = eval(code)
            assert received_co_id == id(self._co_remote_ack), 'co id mismatch ?!'
            remote_ack.set_result(received_co_id)
            return None,

        elif 'coget' == wire_dir:
            # single request/response, rpc style

            coro = self._coget_helper(code)
            co_task = self._loop.create_task(coro)
            return None,  # don't make available to application

        elif 'corun' == wire_dir:
            # flex code/data streaming, pipeline style

            coro = self._corun_helper(code)
            co_task = self._loop.create_task(coro)
            return None,  # don't make available to application

        elif 'wire' == wire_dir:
            # got wire affair packet, land it

            if self._wire_ctx is None:  # populate this wire's ctx jit
                self._wire_ctx = {attr: getattr(self, attr) for attr in dir(self)}

                # universal functions for HBI peers in different languages
                self._wire_ctx['handlePeerErr'] = self._handle_peer_error

            defs = {}
            try:
                affair = run_in_context(code, self._wire_ctx, defs)
                return affair,  # not giving affair to application layer
            except Exception as exc:
                logger.debug(rf'''
HBI {self.net_info}, error landing wire code:
--CODE--
{code!s}
--DEFS--
{defs!r}
--====--
''', exc_info=True)
                self._handle_wire_error(exc)
                # treat wire error as void protocol affair, giving NO value to application layer
                return None,

                # allow customization of code landing
        lander = self.context.get('__hbi_land__', None)
        if lander is not None:
            assert callable(lander), 'non-callable __hbi_land__ defined ?!'
            try:
                landed = lander(code, wire_dir)
                if NotImplemented is not landed:
                    # custom lander can return `NotImplemented` to proceed standard landing
                    return None, landed
            except Exception as exc:
                logger.debug(rf'''
HBI {self.net_info}, error custom landing code:
--CODE--
{code!s}
--====--
''', exc_info=True)
                self._handle_landing_error(exc)
                return exc,

        # standard landing
        if wire_dir is None or len(wire_dir) <= 0:
            # got plain packet, land it

            defs = {}
            try:

                maybe_coro = run_in_context(code, self.context, defs)
                if inspect.iscoroutine(maybe_coro):
                    co_task = self._loop.create_task(maybe_coro)
                    return None, co_task, maybe_coro

                return None, maybe_coro

            except Exception as exc:
                logger.debug(rf'''
HBI {self.net_info}, error landing code:
--CODE--
{code!s}
--DEFS--
{defs!r}
--====--
''', exc_info=True)
                # try handle the error by hbic class
                self._handle_landing_error(exc)
                # return the err so if a coro running, it has a chance to capture it
                return exc, None

        else:
            raise RuntimeError(f'HBI unknown wire directive [{wire_dir}]')

    def _begin_offload(self, sink):
        if self._data_sink is not None:
            raise RuntimeError('HBI already offloading data')
        if not callable(sink):
            raise RuntimeError('HBI sink to offload data must be a function accepting data chunks')
        self._data_sink = sink
        if self._recv_buffer.nbytes > 0:
            # having buffered data, dump to sink
            while self._data_sink is sink:
                chunk = self._recv_buffer.popleft()
                # make sure data keep flowing in regarding lwm
                if self._recv_water_pos() <= self.low_water_mark_recv:
                    self._resume_recv()
                if not chunk:
                    break
                sink(chunk)
        else:
            sink(b'')

    def _end_offload(self, read_ahead=None, sink=None):
        if sink is not None and sink is not self._data_sink:
            raise RuntimeError('HBI resuming from wrong sink')
        self._data_sink = None
        if read_ahead:
            self._recv_buffer.appendleft(read_ahead)
        # this should have been called from a receiving loop or coroutine,
        # so return here, and the recv buffer kept being processed,
        # or the coroutine proceed
        pass

    async def _recv_data(self, bufs):
        fut = self._loop.create_future()

        # use a generator function to pull all buffers from hierarchy
        def pull_from(boc):
            b = self.cast_to_tgt_buffer(boc)  # this static method can be overridden by subclass
            if b:
                yield b
            else:
                for boc1 in boc:
                    yield from pull_from(boc1)

        pos = 0
        buf = None

        buf_puller = pull_from(bufs)

        def data_sink(chunk):
            nonlocal pos
            nonlocal buf

            if chunk is None:
                if not fut.done():
                    fut.set_exception(RuntimeError('HBI disconnected'))
                self._end_offload(None, data_sink)

            try:
                while True:
                    if buf is not None:
                        assert pos < len(buf)
                        # current buffer not filled yet
                        if not chunk or len(chunk) <= 0:
                            # data expected by buf, and none passed in to this call,
                            # return and expect next call into here
                            self._resume_recv()
                            return
                        available = len(chunk)
                        needs = len(buf) - pos
                        if available < needs:
                            # add to current buf
                            new_pos = pos + available
                            buf[pos:new_pos] = chunk.data()
                            pos = new_pos
                            # all data in this chunk has been exhausted while current buffer not filled yet
                            # return now and expect succeeding data chunks to come later
                            self._resume_recv()
                            return
                        # got enough or more data in this chunk to filling current buf
                        buf[pos:] = chunk.data(0, needs)
                        # slice chunk to remaining data
                        chunk.consume(needs)
                        # clear current buffer pointer
                        buf = None
                        pos = 0
                        # continue to process rest data in chunk, even chunk is empty now, still need to proceed for
                        # finish condition check

                    # pull next buf to fill
                    try:
                        buf = next(buf_puller)
                        pos = 0
                        if len(buf) == 0:  # special case for some empty data
                            buf = None
                    except StopIteration as ret:
                        # all buffers in hierarchy filled, finish receiving
                        self._end_offload(chunk, data_sink)
                        # resolve the future
                        if not fut.done():
                            fut.set_result(bufs)
                        # and done
                        return
                    except Exception as exc:
                        raise RuntimeError('HBI buffer source raised exception') from exc
            except Exception as exc:
                self._handle_wire_error(exc)
                if not fut.done():
                    fut.set_exception(exc)

        self._begin_offload(data_sink)

        return await fut

    async def co_recv_data(self, bufs):
        """
        Receive a bulk of binary data from remote corun conversation.

        Must in a corun conversation or RuntimeError will raise.

        """
        if self._co_remote_ack is not None:
            # in active corun conversation, remote may not have acked suspension
            # of other sendings, must wait remote's ack before start receiving
            # for current local conversation.
            local_co_id = await self._co_remote_ack
        else:
            # not in active corun conversation
            if self._co_local_ack is None or self._co_local_done is None:
                # nor in passive corun conversation
                raise RuntimeError('Not in corun conversation!')
            # in passive corun conversation
            if self._co_local_done.done():
                # this actually possible ?
                raise RuntimeError('Passive corun conversation has ended!')
            # remote peer won't send out-of-conversion data before co_end,
            # so it's okay for pipelined receiving for current conversation,
            # whether ack from local has been put on wire or not.

        await self._recv_data(bufs)

    async def co_recv_obj(self):
        """
        Receive the result of local execution of a piece code sent by remote
        conversation.

        Must in a corun conversation or RuntimeError will raise.

        """
        if self._co_remote_ack is not None:
            # in active corun conversation, remote may not have acked suspension
            # of other sendings, must wait remote's ack before start receiving
            # for current local conversation.
            local_co_id = await self._co_remote_ack
        else:
            # not in active corun conversation
            if self._co_local_ack is None or self._co_local_done is None:
                # nor in passive corun conversation
                raise RuntimeError('Not in corun conversation!')
            # in passive corun conversation
            if self._co_local_done.done():
                # this actually possible ?
                raise RuntimeError('Passive corun conversation has ended!')
            # remote peer won't send out-of-conversion data before co_end,
            # so it's okay for pipelined receiving for current conversation,
            # whether ack from local has been put on wire or not.

        fut = self._loop.create_future()
        self._recv_obj_waiters.append(fut)
        self._read_wire()
        return await fut

    def _data_received(self, chunk):
        # push to buffer
        if chunk:
            self._recv_buffer.append(BytesBuffer(chunk))

        # read wire regarding corun/burst mode and flow ctrl
        self._read_wire()

    def _read_wire(self):
        while True:
            # feed as much buffered data as possible to data sink if present
            while self._data_sink:
                # make sure data keep flowing in regarding lwm
                if self._recv_water_pos() <= self.low_water_mark_recv:
                    self._resume_recv()

                if self._recv_buffer is None:
                    # unexpected disconnect
                    self._data_sink(None)
                    return

                chunk = self._recv_buffer.popleft()
                if not chunk:
                    # no more buffered data, wire is empty, return
                    return
                self._data_sink(chunk)

            # try consume all landed but pending consumed objects first
            while self._landed_queue:
                while self._recv_obj_waiters:
                    obj_waiter = self._recv_obj_waiters.popleft()
                    if obj_waiter.done():
                        assert obj_waiter.cancelled() or obj_waiter.exception is not None, \
                            'got result already ?!'
                        # ignore a waiter whether it is cancelled or met other exceptions
                        continue  # find next waiter to receive the landed value
                    landed = self._landed_queue.popleft()
                    if len(landed) == 3:
                        co_task = landed[1]
                        chain_future(co_task, obj_waiter)
                    else:
                        assert len(landed) == 2, '?!'
                        if landed[0] is None:
                            obj_waiter.set_result(landed[1])
                        else:
                            obj_waiter.set_exception(landed[0])
                    if not self._landed_queue:
                        break
                if not self._recv_obj_waiters:
                    break

            while True:
                if self._disconnecting:
                    return

                # ctrl incoming flow regarding hwm/lwm
                buffered_amount = self._recv_water_pos()
                if buffered_amount > self.high_water_mark_recv:
                    self._pause_recv()
                elif buffered_amount <= self.low_water_mark_recv:
                    self._resume_recv()

                # land any packet available from wire
                landed = self._land_one()
                if landed is None:
                    # no more packet to land
                    return
                if len(landed) == 1:
                    # this landed packet is not interesting to application layer
                    continue
                if len(landed) not in (2, 3):
                    raise RuntimeError(
                        f'land result is {type(landed).__name__} of {len(landed)} ?!'
                    )

                if self._co_remote_ack is None and self._co_local_ack is None:
                    # ignore landed result if not in active or passive corun conversation
                    pass
                else:
                    # queue landed packet
                    self._landed_queue.append(landed)

                    if len(self._landed_queue) >= self.app_queue_size:
                        # stop reading wire and pause network recv if landing queue becomes big
                        self._pause_recv()
                        return

                # try give queue head to a waiter
                if not self._recv_obj_waiters or not self._landed_queue:
                    # not possible
                    continue
                landed = self._landed_queue.popleft()
                given_to_a_waiter = False
                while self._recv_obj_waiters:
                    obj_waiter = self._recv_obj_waiters.popleft()
                    if obj_waiter.done():
                        assert obj_waiter.cancelled() or obj_waiter.exception is not None, \
                            'got result already ?!'
                        # ignore a waiter whether it is cancelled or met other exceptions
                        continue  # find next waiter to receive the landed value
                    if len(landed) == 3:
                        co_task = landed[1]
                        chain_future(co_task, obj_waiter)
                    else:
                        assert len(landed) == 2, '?!'
                        if landed[0] is None:
                            obj_waiter.set_result(landed[1])
                        else:
                            obj_waiter.set_exception(landed[0])
                    given_to_a_waiter = True
                    break  # landed value given to a waiter, done for it
                if not given_to_a_waiter:
                    # put back to queue head if not consumed by a waiter
                    self._landed_queue.appendleft(landed)

    def co(self):
        """
        Manage local corun conversations as context over this HBI connection

        """
        return _CoHBIC(self)


class _CoHBIC:
    __slots__ = 'hbic', 'co_req', 'co_ack',

    def __init__(self, hbic: AbstractHBIC):
        self.hbic = hbic
        self.co_req = hbic.loop.create_future()
        self.co_ack = hbic.loop.create_future()

    async def __aenter__(self):
        hbic = self.hbic
        await hbic._send_mutex.acquire(), await hbic._corun_mutex.acquire()
        assert hbic._co_remote_ack is None, 'co act not cleaned ?!'
        co_ack = hbic._co_remote_ack = self.co_ack
        local_co_id = id(co_ack)
        await hbic._send_text(repr(local_co_id), b'co_begin')
        await co_ack
        return hbic

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        hbic = self.hbic
        co_ack = hbic._co_remote_ack
        if co_ack is None:
            # got no chance to lock the mutex, not to release either
            return
        assert co_ack is self.co_ack, 'context corrupted ?!'
        hbic._co_remote_ack = None
        if hbic._landed_queue:
            logger.warning(
                f'Non-empty app queue ({len(hbic._landed_queue)} objects)'
                f' at end of active conversation.'
            )
            hbic._landed_queue.clear()
        local_co_id = id(co_ack)
        await hbic._send_text(repr(local_co_id), b'co_end')
        hbic._corun_mutex.release()
        hbic._send_mutex.release()
