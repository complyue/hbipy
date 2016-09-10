from typing import *
import abc
import asyncio
import functools
import inspect
import logging
from collections import deque, OrderedDict
import contextlib

from .buflist import BufferList
from .context import run_in_context
from .exceptions import *
from .sendctrl import SendCtrl

logger = logging.getLogger(__name__)

# max scanned length of packet header
PACK_HEADER_MAX = 60
PACK_BEGIN = b'['
PACK_LEN_END = b'#'
PACK_END = b']'

DEFAULT_DISCONNECT_WAIT = 30


class HBIConnectionListener(metaclass=abc.ABCMeta):
    def connected(self, hbic):
        """
        :param hbic:
        :return: False to remove this listener, else keep
        """
        pass

    def disconnected(self, hbic):
        """
        :param hbic:
        :return: False to remove this listener, else keep
        """
        pass


class AbstractHBIC:
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
            raise UsageError('bytes can not be target buffer since readonly')
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
            raise UsageError('readonly memoryview can not be target buffer')
        if boc.nbytes == 0:  # if zero-length, replace with empty bytes
            # coz when a zero length ndarray is viewed, cast/send will raise while not needed at all
            return b''
        elif boc.itemsize != 1:
            return boc.cast('B')
        return boc

    addr = None
    net_opts = None
    hbic_listener = None
    transport = None
    _wire_ctx = None
    _loop = None

    _data_sink = None

    def __init__(self, context, addr=None, net_opts=None, *, hbic_listener=None,
                 send_only=False, auto_connect=False, reconn_delay=10,
                 low_water_mark_send=2 * 1024 * 1024, high_water_mark_send=12 * 1024 * 1024,
                 low_water_mark_recv=2 * 1024 * 1024, high_water_mark_recv=12 * 1024 * 1024,
                 loop=None, **kwargs):
        super().__init__(**kwargs)
        self.context = context
        if addr is not None:
            self.addr = addr
        if net_opts is not None:
            self.net_opts = net_opts
        if hbic_listener is not None:
            self.hbic_listener = hbic_listener
        self._conn_listeners = OrderedDict()
        self.send_only = send_only
        self.auto_connect = auto_connect
        self.reconn_delay = reconn_delay
        if loop is None:
            loop = asyncio.get_event_loop()
        self._loop = loop

        self.low_water_mark_send = low_water_mark_send
        self.high_water_mark_send = high_water_mark_send
        self._send_mutex = SendCtrl(loop=loop)

        self._recv_buffer = None
        self.low_water_mark_recv = low_water_mark_recv
        self.high_water_mark_recv = high_water_mark_recv
        self._recv_obj_waiters = deque()
        self._corun_mutex = asyncio.Lock(loop=loop)

        self._conn_waiters = deque()
        if auto_connect:
            loop.call_soon(self.connect)

    def _handle_landing_error(self, exc):
        # disconnect anyway
        self.disconnect(exc)

        if self._corun_mutex.locked():
            # coro running, let it handle the err
            logger.warn({'err': exc}, 'landing error')
        else:
            # in hosting mode, forcefully disconnect
            logger.fatal({'err': exc}, 'disconnecting due to landing error')

    def _handle_peer_error(self, message, stack):
        logger.warn('disconnecting due to peer error: {}\n{}'.format(message, stack))
        self.disconnect()

    def _handle_wire_error(self, exc, transport=None):
        self.disconnect(exc, 0, transport)

    @property
    def connected(self):
        transport = self.transport
        if not transport:
            return False
        raise NotImplementedError

    @property
    def net_info(self):
        if not self.addr:
            return '<destroyed'
        transport = self.transport
        if not transport:
            return '<unwired>'
        return '[hbic wired to ' + transport + ']'

    def add_connection_listener(self, listener):
        self._conn_listeners[listener] = None

    def remove_connection_listener(self, listener):
        del self._conn_listeners[listener]

    def wire(self, transport):
        self.transport = transport
        self._recv_buffer = BufferList()
        if self.hbic_listener:
            self.hbic_listener(self)
        self._send_mutex.startup()

        # notify listeners
        for cl in tuple(self._conn_listeners.keys()):
            if False is cl.connected(self):
                del self._conn_listeners[cl]

    def unwire(self, transport, exc=None):
        if transport is not self.transport:
            return
        self.transport = None
        self._send_mutex.shutdown(exc)
        if exc:
            logger.warn('connection unwired due to error', {'err': exc})

        # abort pending tasks
        self._recv_buffer = None
        if self._recv_obj_waiters:
            if not exc:
                try:
                    raise WireError('disconnected')
                except WireError as the_exc:
                    exc = the_exc
            waiters = self._recv_obj_waiters
            self._recv_obj_waiters = deque()
            for waiter in waiters:
                waiter.set_exception(exc)

        # notify listeners
        for cl in tuple(self._conn_listeners):
            if False is cl.disconnected(self):
                del self._conn_listeners[cl]

        # attempt auto re-connection
        if self.auto_connect:
            self._loop.call_later(self.reconn_delay, self._reconn)

    def disconnect(self, err_reason=None, destroy_delay=DEFAULT_DISCONNECT_WAIT, transport=None):
        raise NotImplementedError

    def _reconn(self):
        if not self.addr:
            return
        self._loop.create_task(self.connect())

    async def connect(self, addr=None, net_opts=None):
        if self.connected and (addr is None or addr == self.addr) and (net_opts is None or net_opts == self.net_opts):
            return self
        if addr is not None:
            self.addr = addr
        if net_opts is not None:
            self.net_opts = None
        if not self.addr:
            raise asyncio.InvalidStateError('attempt connecting without addr')
        try:
            if self.connected:
                with await (self._send_mutex), await (
                        self._corun_mutex):  # lock mutexes to wait all previous sending finished
                    self.disconnect(None, 0)
            fut = self._loop.create_future()
            self._conn_waiters.append(fut)
            self._connect()
            await fut
            return self
        except Exception:
            # attempt auto re-connection
            if self.auto_connect:
                self._loop.call_later(self.reconn_delay, self._reconn)

    def _connect(self):
        raise NotImplementedError

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
            await self._send_data(bufs)

    async def co_send_code(self, code, wire_dir=None):
        if not self._corun_mutex.locked():
            raise UsageError('not in corun mode')
        await self._send_code(code, wire_dir)

    async def co_send_data(self, bufs):
        if not self._corun_mutex.locked():
            raise UsageError('not in corun mode')
        await self._send_data(bufs)

    async def _send_code(self, code, wire_dir=None):
        if not self.connected:
            raise WireError('not connected')

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
        # use a generator function to pull all buffers from hierarchy
        def pull_from(boc):
            b = self.cast_to_src_buffer(boc)  # this static method can be overridden by subclass
            if b is not None:
                yield b
                return
            for boc1 in boc:
                yield from pull_from(boc1)

        for buf in pull_from(bufs):
            await self._send_buffer(buf)

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

    async def _land_one(self):
        raise NotImplementedError

    def land(self, code, wire_dir):
        if wire_dir is None or len(wire_dir) <= 0:
            # got plain packet, land it
            defs = {}
            self.context['_peer_'] = self
            try:
                return None, run_in_context(code, self.context, defs)
            except Exception as exc:
                # try handle the error by hbic class
                self._handle_landing_error(exc)
                # return the err so the running coro also has a chance to handle it
                return exc,
            finally:
                self.context['_peer_'] = None
        if 'corun' == wire_dir:
            # land the coro and run it
            defs = {}
            self.context['_peer_'] = self
            try:
                coro = run_in_context(code, self.context, defs)
                ctask = self.corun(coro)

                def clear_peer(fut):  # can only be cleared when coro finished, or it'll lose context
                    self.context['_peer_'] = None

                ctask.add_done_callback(clear_peer)
                return None, ctask
            except Exception as exc:
                return exc,
            finally:
                if len(defs) > 0:
                    logger.debug({"defs": defs, "code": code}, 'sth defined by landed corun code.')
        elif 'wire' == wire_dir:
            # got wire affair packet, land it
            if self._wire_ctx is None:  # populate this wire's ctx jit
                self._wire_ctx = {}
                for attr in dir(self):
                    self._wire_ctx[attr] = getattr(self, attr)
                self._wire_ctx['handlePeerErr'] = self._handle_peer_error  # function name from hbi js
            defs = {}
            try:
                run_in_context(code, self._wire_ctx, defs)
            except Exception as exc:
                self._handle_wire_error(exc)
            finally:
                if len(defs) > 0:
                    logger.warn('landed wire code defines sth.', {"defs": defs, "code": code})
        else:
            raise WireError('unknown wire directive [{}]'.format(wire_dir))

    def _begin_offload(self, sink):
        if self._data_sink is not None:
            raise UsageError('already offloading data')
        if not callable(sink):
            raise UsageError('HBI sink to offload data must be a function accepting data chunks')
        self._data_sink = sink
        if self._recv_buffer.nbytes > 0:
            # having buffered data, dump to sink
            while self._data_sink is sink:
                chunk = self._recv_buffer.popleft()
                if not chunk:
                    break
                sink(chunk)
        else:
            sink(b'')

    def _end_offload(self, read_ahead=None, sink=None):
        if sink is not None and sink is not self._data_sink:
            raise UsageError('resuming from wrong sink')
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
                fut.set_exception(WireError('disconnected'))
                self._end_offload(None, data_sink)

            try:
                while True:
                    if buf is not None:
                        assert pos < len(buf)
                        # current buffer not filled yet
                        if not chunk or len(chunk) <= 0:
                            # data expected by buf, and none passed in to this call, return and expect next call into here
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
                        raise UsageError('buffer source raised exception') from exc
            except Exception as exc:
                self._handle_wire_error(exc)
                fut.set_exception(exc)

        self._begin_offload(data_sink)

        return await fut

    async def recv_data(self, bufs):
        if self._corun_mutex.locked():
            raise UsageError('this should only be used in hosting mode')
        await self._recv_data(bufs)

    async def co_recv_data(self, bufs):
        if not self._corun_mutex.locked():
            raise UsageError('not in corun mode')
        await self._recv_data(bufs)

    async def co_recv_obj(self):
        if not self._corun_mutex.locked():
            raise UsageError('not in corun mode')

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
                        if self._corun_mutex.locked():
                            # switched to corun mode during landing, stop draining wire, let the coro recv on-demand,
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
            self.disconnect(exc, 0)

    def corun(self, coro):
        """
Run a coroutine, while having this HBIC track its execution.

As the coroutine is launched, this HBIC switches itself into `corun` mode. In contrast to normal `hosting` mode,
packets will not get landed immediately at arrival, both for code and binary data. The transport will build back
pressure as pending amount raises beyond `high_water_mark_recv`. The coroutine is expected to read packet landing
results and binary data by awaiting calls to `co_recv_obj` and `co_recv_data`. When the coroutine terminates either by
returning or letting through unhandled exception, this HBIC will launch next coroutine if in queue, or resume hosting
mode.
"""

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
            await self._send_code(code, 'corun')
            if bufs is not None:
                await self._send_data(bufs)
        else:
            with await self._send_mutex:
                await self._send_code(code, 'corun')
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
