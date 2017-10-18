import asyncio
import contextlib
import functools
import inspect
import logging
from collections import deque
from typing import Sequence

from .buflist import *
from .context import run_in_context
from .sendctrl import SendCtrl

__all__ = [
    'corun_with',
    'AbstractHBIC',
]

logger = logging.getLogger(__name__)


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
            context, addr=None, net_opts=None,
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
        self._loop.create_task(self._disconnect(exc, err_stack))

    def _handle_landing_error(self, exc):
        import traceback
        traceback.print_exc()
        logger.fatal(f'HBI {self.net_info} landing error occurred, forcing disconnection.')
        err_stack = traceback.format_exc()
        self._loop.create_task(self._disconnect(exc, err_stack))

    def _handle_peer_error(self, message, stack=None):
        self.disconnect(f'Peer error: {message}', stack)

    @property
    def loop(self):
        return self._loop

    @property
    def connected(self):
        return self._wire is not None

    @property
    def net_info(self):
        if not self.addr:
            return '<destroyed>'
        _wire = self._wire
        if not _wire:
            return '<unwired>'
        return f'[{_wire}]'

    def run_until_disconnected(self, ensure_connected=True):
        if not self.connected:
            # already disconnected

            if ensure_connected:
                # initiate connection now
                self.connect()
            else:
                # nop in this case
                return

        if self._disc_fut is None:
            self._disc_fut = self._loop.create_future()
        self._loop.run_until_complete(self._disc_fut)

    def disconnect(self, err_reason=None, err_stack=None):
        if err_reason is not None:
            if err_stack is None and isinstance(err_reason, BaseException):
                import traceback
                err_stack = traceback.format_exc()

            logger.fatal(rf'''
HBI disconnecting {self.net_info} due to error: {err_reason}
{err_stack or ''}
''')

        disconn_cb = self.context.get('hbi_disconnecting', None)
        if disconn_cb is not None:
            disconn_cb(err_reason)

        # this can be called from any thread
        self._loop.call_soon_threadsafe(self._loop.create_task, self._disconnect(err_reason, err_stack))

    async def wait_disconnected(self):
        if self._disc_fut is None:
            self._disc_fut = self._loop.create_future()
        await self._disc_fut

    def _disconnect(self, err_reason=None, err_stack=None):
        raise NotImplementedError('subclass should implement this as a coroutine')

    def _disconnected(self, exc=None):
        if exc:
            logger.warning(f'HBI connection unwired due to error: {exc}', exc_info=True)

        # abort pending tasks
        self._recv_buffer = None
        if self._recv_obj_waiters:
            if not exc:
                exc = RuntimeError('HBI disconnected')
            waiters = self._recv_obj_waiters
            self._recv_obj_waiters = deque()
            for waiter in waiters:
                waiter.set_exception(exc)

        self._send_mutex.shutdown(exc)

        fut = self._disc_fut
        if fut is not None:
            self._disc_fut = None
            fut.set_result(self)

        disconn_cb = self.context.get('hbi_disconnected', None)
        if disconn_cb is not None:
            disconn_cb()

    def _peer_eof(self):
        peer_done_cb = self.context.get('hbi_peer_done', None)
        if peer_done_cb is not None:
            peer_done_cb()

    def _connect(self):
        raise NotImplementedError('subclass should implement this as a coroutine')

    def connect(self, addr=None, net_opts=None):
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
            conn_cb()

    async def wait_connected(self):
        if self.connected:
            return

        if self._conn_fut is None:
            self._conn_fut = self._loop.create_future()
        await self._conn_fut

    def run_until_connected(self):
        self.connect()
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
        raise NotImplementedError

    def _pause_recv(self):
        raise NotImplementedError

    def _resume_recv(self):
        raise NotImplementedError

    def _land_one(self):
        raise NotImplementedError

    def land(self, code, wire_dir):

        # allow customization of code landing
        lander = self.context.get('__hbi_land__', None)
        if lander is not None:
            assert callable(lander), 'non-callable __hbi_land__ defined ?!'
            try:
                if NotImplemented is not lander(code, wire_dir):
                    # custom lander can return `NotImplemented` to proceed standard landing
                    return
            except Exception as exc:
                # treat uncaught error in custom landing as wire error
                self._handle_wire_error(exc)
                return

        # standard landing

        if wire_dir is None or len(wire_dir) <= 0:
            # got plain packet, land it
            defs = {}
            try:
                return None, run_in_context(code, self.context, defs)
            except Exception as exc:
                # try handle the error by hbic class
                self._handle_landing_error(exc)
                # return the err so the running coro also has a chance to handle it
                return exc,
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
            # land the coro and run it
            defs = {}
            try:
                coro = run_in_context(code, self.context, defs)
                ctask = self.corun(coro)
                return None, ctask, coro
            except Exception as exc:
                self._handle_wire_error(exc)
                return exc,
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
                run_in_context(code, self._wire_ctx, defs)
            except Exception as exc:
                self._handle_wire_error(exc)
            finally:
                if len(defs) > 0:
                    logger.warning('landed wire code defines sth.', {"defs": defs, "code": code})
        else:
            raise RuntimeError('HBI unknown wire directive [{}]'.format(wire_dir))

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
                        if buf.nbytes == 0:  # special case for some empty data
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
            raise RuntimeError('HBI recv_data should only be used in hosting mode')
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
            got = self._land_one()
            if got is None:
                # no obj available for now
                break
            if len(self._recv_obj_waiters) > 0:
                # feed previous waiters first
                obj_waiter = self._recv_obj_waiters.popleft()
                if got[0] is None:
                    obj_waiter.set_result(got[1])
                else:
                    obj_waiter.set_exception(got[0])
                continue

            # all waiters resolved, got this one to return
            if got[0] is None:
                return got[1]
            else:
                raise got[0]

        # no object from buffer available for now, queue as a waiter, await the future
        fut = self._loop.create_future()
        self._recv_obj_waiters.append(fut)
        self._read_wire()
        return await fut

    def _read_wire(self):
        try:
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

                # in hosting mode, make the incoming data flew as fast as possible
                if not self._corun_mutex.locked():
                    # try consume all buffered data first
                    while True:
                        # meanwhile in hosting mode, make sure data keep flowing in regarding lwm
                        if self._recv_water_pos() <= self.low_water_mark_recv:
                            self._resume_recv()
                        got = self._land_one()
                        if not got:
                            # no more packet to land
                            break
                        if len(got) > 2 and asyncio.iscoroutine(got[2]):
                            # started a coro during landing, stop draining wire, let the coro recv on-demand,
                            # and let subsequent incoming data to trigger hwm back pressure
                            return
                    return

                # currently in corun mode
                # first, try landing as many packets as awaited from buffered data
                while len(self._recv_obj_waiters) > 0:
                    obj_waiter = self._recv_obj_waiters.popleft()
                    got = self._land_one()
                    if got is None:
                        # no obj from buffer for now
                        self._recv_obj_waiters.appendleft(obj_waiter)
                        break
                    if got[0] is None:
                        obj_waiter.set_result(got[1])
                    else:
                        obj_waiter.set_exception(got[0])
                    if not self._corun_mutex.locked():
                        # switched to hosting during landing, just settled waiter should be the last one being awaited
                        assert len(self._recv_obj_waiters) == 0
                        break

                # and if still in corun mode, i.e. not finally switched to hosting mode by previous landings
                if self._corun_mutex.locked():
                    # ctrl incoming flow regarding hwm/lwm
                    buffered_amount = self._recv_water_pos()
                    if buffered_amount > self.high_water_mark_recv:
                        self._pause_recv()
                    elif buffered_amount <= self.low_water_mark_recv:
                        self._resume_recv()
                    # return now and will be called on subsequent recv demand
                    return
        except Exception as exc:
            self._handle_wire_error(exc)
            raise

    def corun(self, coro):

        @functools.wraps(coro)
        async def wrapper():
            # use send+corun mutex to prevent interference with other sendings or coros
            with await self._send_mutex, await self._corun_mutex:
                try:
                    return await coro
                except Exception as exc:
                    self._handle_landing_error(exc)
                    raise

        return self._loop.create_task(wrapper())

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


def corun_with(coro, hbics: Sequence[AbstractHBIC]):
    loop = None
    for hbic in hbics:
        if loop is None:
            loop = hbic._loop
        elif loop is not hbic._loop:
            raise Exception('corun with hbics on different loops is not possible')

    @functools.wraps(coro)
    async def wrapper():
        with contextlib.ExitStack() as stack:
            # use mutexes of all hbics
            for hbic in hbics:
                # use send+corun mutex to prevent interference with other sendings or coros
                stack.enter_context(await hbic._send_mutex)
                stack.enter_context(await hbic._corun_mutex)

            try:
                return await coro
            except Exception as exc:
                for hbic in hbics:
                    hbic._handle_landing_error(exc)
                raise

    return loop.create_task(wrapper())
