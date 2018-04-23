import asyncio
import contextlib
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
    'corun_with', 'run_coro_with',
]

logger = logging.getLogger(__name__)


def resolve_coro(coro, to_fut):
    assert asyncio.isfuture(coro)

    if to_fut.done():
        # target already cancelled etc.
        return

    def coro_cb(coro_fut):

        if to_fut.done():
            # target already cancelled etc.
            return

        if coro_fut.cancelled():
            to_fut.cancel()
        elif coro_fut.exception() is not None:
            to_fut.set_exception(coro_fut.exception())
        else:
            fr = coro_fut.result()
            if asyncio.isfuture(fr):
                # continue the coro chain
                resolve_coro(fr, to_fut)
            else:
                # just resolved
                to_fut.set_result(fr)

    asyncio.ensure_future(coro).add_done_callback(coro_cb)


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
        '_wire', '_wire_ctx', '_data_sink', '_conn_fut', '_disc_fut',

        'low_water_mark_send', 'high_water_mark_send', '_send_mutex',

        '_recv_buffer',
        'low_water_mark_recv', 'high_water_mark_recv',
        '_recv_obj_waiters',

        '_corun_mutex',
    )

    def __init__(
            self,
            context,
            addr=None, net_opts=None,
            *,
            send_only=False,
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

        self._wire = None
        self._wire_ctx = None
        self._data_sink = None
        self._conn_fut = None
        self._disc_fut = None

        self.low_water_mark_send = low_water_mark_send
        self.high_water_mark_send = high_water_mark_send
        self._send_mutex = SendCtrl(loop=loop)

        self._recv_buffer = None
        self.low_water_mark_recv = low_water_mark_recv
        self.high_water_mark_recv = high_water_mark_recv
        self._recv_obj_waiters = deque()

        self._corun_mutex = asyncio.Lock(loop=loop)

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

            # if in corun mode, leave the error to be thrown into the coro running
            if self._corun_mutex.locked():
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
        self._disconnecting = True

        if err_reason is not None:
            if err_stack is None and isinstance(err_reason, BaseException):
                import traceback
                err_stack = traceback.format_exc()

            logger.error(rf'''
HBI disconnecting {self.net_info} due to error: {err_reason}
{err_stack or ''}
''')

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
            if (addr is None or addr == self.addr) and (net_opts is None or net_opts == self.net_opts):
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

    def fire(self, code, bufs=None):
        """
        Fire and forget.

        """
        self._loop.call_soon_threadsafe(self._loop.create_task, self.convey(code, bufs))

    def fire_corun(self, code, bufs=None):
        """
        Fire and forget.

        """
        self._loop.call_soon_threadsafe(self._loop.create_task, self.send_corun(code, bufs))

    async def send_code(self, code, wire_dir=None):
        # use mutex to prevent interference
        with await self._send_mutex:
            await self._send_code(code, wire_dir)

    async def send_data(self, bufs):
        # use mutex to prevent interference
        with await self._send_mutex:
            await self._send_data(bufs)

    async def convey(self, code, bufs=None):
        # use mutex to prevent interference
        with await self._send_mutex:
            await self._send_code(code)
            if bufs is not None:
                await self._send_data(bufs)

    async def co_send_code(self, code, wire_dir=None):
        if not self._corun_mutex.locked():
            raise RuntimeError('HBI not in corun mode')
        await self._send_code(code, wire_dir)

    async def co_send_data(self, bufs):
        if not self._corun_mutex.locked():
            raise RuntimeError('HBI not in corun mode')
        await self._send_data(bufs)

    async def _send_code(self, code, wire_dir=None):
        if not self.connected:
            raise RuntimeError('HBI wire not connected')

        # convert wire_dir as early as possible, will save conversions in case the code is of complex structure
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

    def land(self, code, wire_dir) -> Optional[tuple]:
        # Semantic of return value from this function is same as _land_one()

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
''')
                # try handle the error by hbic class
                self._handle_landing_error(exc)
                # return the err so if a coro running, it has a chance to capture it
                return exc, None

            finally:
                if len(defs) > 0:
                    logger.debug(rf'''
HBI {self.net_info}, landed code defined something:
--CODE--
{code!s}
--DEFS--
{defs!r}
--====--
''')

        if 'corun' == wire_dir:
            # land the coro and start a task to run it
            defs = {}
            co_task, coro = None, None
            try:
                coro = run_in_context(code, self.context, defs)
                co_task = self.corun(coro)
                return None, co_task, coro
            except Exception as exc:
                self._handle_landing_error(exc)
                return exc, co_task, coro
            finally:
                if len(defs) > 0:
                    logger.debug('sth defined by landed corun code.', {"defs": defs, "code": code})
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
                self._handle_wire_error(exc)
                return None,  # treat wire error as void protocol affair, giving NO value to application layer
            finally:
                if len(defs) > 0:
                    logger.warning('landed wire code defines sth.', {"defs": defs, "code": code})
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
        # this should have been called from a receiving loop or coroutine, so return here should allow either continue
        # processing the recv buffer, or the coroutine proceed
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
                        fut.set_result(bufs)
                        # and done
                        return
                    except Exception as exc:
                        raise RuntimeError('HBI buffer source raised exception') from exc
            except Exception as exc:
                self._handle_wire_error(exc)
                fut.set_exception(exc)

        self._begin_offload(data_sink)

        return await fut

    async def recv_data(self, bufs):
        if self._corun_mutex.locked():
            raise RuntimeError('HBI recv_data should only be used in burst mode')
        await self._recv_data(bufs)

    async def co_recv_data(self, bufs):
        if not self._corun_mutex.locked():
            raise RuntimeError('HBI not in corun mode')
        await self._recv_data(bufs)

    async def co_recv_obj(self):
        if not self._corun_mutex.locked():
            raise RuntimeError('HBI not in corun mode')

        while True:
            # continue poll one obj from buffer
            landed = self._land_one()
            if landed is None:
                # no more packet to land
                break
            if len(landed) == 1:
                # this landed packet is not interesting to application layer
                continue
            if len(landed) == 3:
                # a new coro task started during co-run
                co_task = landed[1]
                assert not co_task.done(), 'co task done immediately as landed ?!'
                # TODO: preemptive execution of such a coro task may be necessary if binary data follows,
                # TODO: but currently this new coro task will be blocked by _corun_mutext,
                # TODO: until current coro task or co ctx block finishes running.
                continue
            assert len(landed) == 2, f'land result is {type(landed).__name__} of {len(landed)} ?!'
            if len(self._recv_obj_waiters) > 0:
                # feed previous waiters first
                obj_waiter = self._recv_obj_waiters.popleft()
                if obj_waiter.done():
                    if obj_waiter.cancelled():
                        logger.warning(f'A landed value discarded as not given to a cancelled co_recv_obj() call.')
                    elif obj_waiter.exception() is not None:
                        logger.error(f'A co_recv_obj() call failed before a value landed for it ?!')
                    else:
                        logger.error(f'A co_recv_obj() call got result before a value landed for it ?!')
                else:
                    if landed[0] is None:
                        obj_waiter.set_result(landed[1])
                    else:
                        obj_waiter.set_exception(landed[0])
                continue

            # all waiters resolved, landed this one to return
            if landed[0] is None:
                return landed[1]
            else:
                raise landed[0]

        # no object from buffer available for now, queue as a waiter, await the future
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

            # in burst mode, make the incoming data flew as fast as possible
            if not self._corun_mutex.locked():
                # try consume all buffered data first
                while True:
                    # meanwhile in burst mode, make sure data keep flowing in regarding lwm
                    if self._recv_water_pos() <= self.low_water_mark_recv:
                        self._resume_recv()
                    landed = self._land_one()
                    if landed is None:
                        # no more packet to land
                        break
                    if len(landed) == 1:
                        # this landed packet is not interesting to application layer
                        continue
                    if len(landed) == 3:
                        # a new coro task started during landing, stop draining wire, let the coro recv on-demand,
                        # and let subsequent incoming data to trigger hwm back pressure
                        return
                    assert len(landed) == 2, f'land result is {type(landed).__name__} of {len(landed)} ?!'
                    if landed[0] is not None:
                        # exception occurred in burst mode
                        if not self._disconnecting:
                            # if not disconnecting as handling result, raise for the event loop to handle it
                            raise landed[0]
                    elif inspect.isawaitable(landed[1]):
                        asyncio.ensure_future(landed[1])
                return

            # currently in corun mode
            if len(self._recv_obj_waiters) <= 0:
                logger.warning(f'Pkt received but NO data waiter on wire during corun ?!')
                # postpone wire reading, with flow ctrl imposed below

            # first, try landing as many packets as awaited from buffered data
            while len(self._recv_obj_waiters) > 0:
                landed = self._land_one()
                if landed is None:
                    # no more packet to land
                    break
                if len(landed) == 1:
                    # this landed packet is not interesting to application layer
                    continue
                if len(landed) == 3:
                    # a new coro task started during co-run
                    co_task = landed[1]
                    assert not co_task.done(), 'co task done immediately as landed ?!'
                    # TODO: preemptive execution of such a coro task may be necessary if binary data follows,
                    # TODO: but currently this new coro task will be blocked by _corun_mutext,
                    # TODO: until current coro task or co ctx block finishes running.
                    continue
                assert len(landed) == 2, f'land result is {type(landed).__name__} of {len(landed)} ?!'
                obj_waiter = self._recv_obj_waiters.popleft()
                if obj_waiter.done():
                    if obj_waiter.cancelled():
                        logger.warning(f'A landed value discarded as not given to a cancelled co_recv_obj() call.')
                    elif obj_waiter.exception() is not None:
                        logger.error(f'A co_recv_obj() call failed before a value landed for it ?!')
                    else:
                        logger.error(f'A co_recv_obj() call got result before a value landed for it ?!')
                else:
                    if landed[0] is None:
                        if inspect.isawaitable(landed[1]):
                            # chain the coros etc.
                            resolve_coro(asyncio.ensure_future(landed[1]), obj_waiter)
                        else:
                            obj_waiter.set_result(landed[1])
                    else:
                        obj_waiter.set_exception(landed[0])
                if not self._corun_mutex.locked():
                    # switched to burst mode during landing, just settled waiter should be the last one being awaited
                    assert len(self._recv_obj_waiters) == 0
                    break

            # and if still in corun mode, i.e. not finally switched to burst mode by previous landings
            if self._corun_mutex.locked():
                # ctrl incoming flow regarding hwm/lwm
                buffered_amount = self._recv_water_pos()
                if buffered_amount > self.high_water_mark_recv:
                    self._pause_recv()
                elif buffered_amount <= self.low_water_mark_recv:
                    self._resume_recv()
                # return now and will be called on subsequent recv demand
                return

    async def _corun(self, coro):
        # use send+corun mutex to prevent interference with other sendings or coros
        with await self._send_mutex, await self._corun_mutex:
            return await coro

    def corun(self, coro):
        """
        Run a coroutine within which `co_*()` methods of this hbic can be called.

        """
        return self._loop.create_task(self._corun(coro))

    def run_coro(self, coro):
        return self._loop.run_until_complete(self._corun(coro))

    async def send_corun(self, code, bufs=None):
        if self._corun_mutex.locked():
            # sending mutex is effectively locked in corun mode
            await self._send_code(code, b'corun')
            if bufs is not None:
                await self._send_data(bufs)
        else:
            with await self._send_mutex:
                await self._send_code(code, b'corun')
                if bufs is not None:
                    await self._send_data(bufs)

    # implement HBIC as reusable context manager

    def __enter__(self):
        self.run_until_connected()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            err_reason = None
            err_stack = None
        else:
            import traceback
            err_reason = exc_val if isinstance(exc_val, BaseException) else exc_type(exc_val)
            err_stack = ''.join(traceback.format_exception(exc_type, exc_val, exc_tb))

        self.disconnect(err_reason, err_stack, try_send_peer_err=True)
        self.run_until_disconnected(ensure_connected=False)

    # implement HBIC as reusable async context manager

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

    def co(self):
        """
        This meant to be used in `async with` as the async context manager to provide a `with` context where `co_*()`
        methods can be called, without (async) defining a separate coro.

        """
        return _CoHBIC(self)


class _CoHBIC:
    __slots__ = ('hbic',)

    def __init__(self, hbic: AbstractHBIC):
        self.hbic = hbic

    async def __aenter__(self):
        await self.hbic._send_mutex.acquire(), await self.hbic._corun_mutex.acquire()
        return self.hbic

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.hbic._corun_mutex.release()
        self.hbic._send_mutex.release()


def _loop_of_hbics(hbics):
    loop = None
    for hbic in hbics:
        if loop is None:
            loop = hbic._loop
        elif loop is not hbic._loop:
            raise RuntimeError('HBI corun with hbics on different loops is not possible')
    return loop


async def _corun_with(coro, hbics):
    with contextlib.ExitStack() as stack:
        # use mutexes of all hbics
        for hbic in hbics:
            # use send+corun mutex to prevent interference with other sendings or coros
            stack.enter_context(await hbic._send_mutex)
            stack.enter_context(await hbic._corun_mutex)
        return await coro


def corun_with(coro, hbics):
    return _loop_of_hbics(hbics).create_task(_corun_with(coro, hbics))


def run_coro_with(coro, hbics):
    _loop_of_hbics(hbics).run_until_complete(_corun_with(coro, hbics))
