import json

from .conn import *
from .exceptions import *
from .bytesbuf import *

logger = logging.getLogger(__name__)


class HBIC(AbstractHBIC, asyncio.Protocol):
    @classmethod
    def create_server(cls, context_factory, addr, net_opts=None, *, loop=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        server = loop.create_server(
            lambda: cls(context_factory(), loop=loop, **kwargs),
            host=addr.get('host'), port=addr['port'], **(net_opts or {}), **kwargs
        )
        return server

    def __init__(self, context, addr=None, net_opts=None, **kwargs):
        super().__init__(context=context, addr=addr, net_opts=net_opts, **kwargs)
        self._recv_paused = False
        self._hdr_buf = None
        self._hdr_got = 0
        self._bdy_buf = None
        self._bdy_got = 0
        self._wire_dir = None

    @property
    def connected(self):
        transport = self.transport
        if not transport:
            return False
        return not transport.is_closing()

    @property
    def net_info(self):
        if not self.addr:
            return '<destroyed>'
        transport = self.transport
        if not transport:
            return '<unwired>'
        if transport.is_closing():
            return '<closing>'
        return '[{!r}] <=> [{!r}]'.format(transport.get_extra_info('sockname'), transport.get_extra_info('peername'))

    def _connect(self):
        assert self.addr, 'should not reach here'
        if self.transport:
            if not self.transport.is_closing():
                raise asyncio.InvalidStateError('requesting new connection with transport already wired')
            self.transport = None  # clear the attr earlier

        async def do_conn():
            try:
                (transport, protocol) = await self._loop.create_connection(
                    lambda: self, self.addr['host'], self.addr['port'], **self.net_opts or {})
            except Exception as exc:
                waiters = self._conn_waiters
                self._conn_waiters = deque()
                for waiter in waiters:
                    if not waiter.done():
                        waiter.set_exception(exc)
            else:
                waiters = self._conn_waiters
                self._conn_waiters = deque()
                for waiter in waiters:
                    if not waiter.done():
                        waiter.set_result(self)

        self._loop.create_task(do_conn())

    async def _send_text(self, code, wire_dir=b''):
        if isinstance(code, bytes):
            payload = code
        elif isinstance(code, str):
            payload = code.encode('utf-8')
        else:
            # try convert to json and send
            payload = json.dumps(code).encode('utf-8')

        await self._send_mutex.flowing()
        self.transport.writelines([
            b'[%d#%s]' % (len(payload), wire_dir),
            payload
        ])

    async def _send_buffer(self, buf):
        # wait sendable for each single buffer
        await self._send_mutex.flowing()
        self.transport.write(buf)

    def _recv_water_pos(self):
        return self._recv_buffer.nbytes

    def _pause_recv(self):
        if self._recv_paused:
            return

        self.transport.pause_reading()
        self._recv_paused = True

    def _resume_recv(self):
        if not self._recv_paused:
            return

        self.transport.resume_reading()
        self._recv_paused = False

    def disconnect(self, err_reason=None, destroy_delay=DEFAULT_DISCONNECT_WAIT, transport=None):
        if transport is None:
            transport = self.transport
        if not transport:
            return
        if transport.is_closing():  # already closing
            return
        self._hdr_got = 0
        self._bdy_buf = None
        self._wire_dir = None
        if err_reason:
            # TODO send peer error before closing transport
            logger.fatal({'err': err_reason}, 'disconnecting wire due to error')
        transport.write_eof()
        transport.close()
        # assume connection_lost to be called by asyncio loop

    def connection_made(self, transport):
        if self.transport:
            if self.transport.is_closing():
                logger.warn('reconnect so fast, old transport not fully closed yet')
            else:
                raise asyncio.InvalidStateError('replacing connected transport with new one')
        transport.set_write_buffer_limits(self.high_water_mark_send, self.low_water_mark_send)
        self._hdr_buf = bytearray(PACK_HEADER_MAX)
        self._hdr_got = 0
        self._bdy_got = None
        self._wire_dir = None
        self.wire(transport)

    def pause_writing(self):
        """this is to implement asyncio.Protocol, never call it directly"""
        self._send_mutex.obstruct()

    def resume_writing(self):
        """this is to implement asyncio.Protocol, never call it directly"""
        self._send_mutex.unleash()

    def connection_lost(self, exc):
        """this is to implement asyncio.Protocol, never call it directly"""
        self.unwire(self.transport, exc)

    def eof_received(self):
        """this is to implement asyncio.Protocol, never call it directly"""
        self.disconnect()

    def data_received(self, chunk):
        """this is to implement asyncio.Protocol, never call it directly"""

        # push to buffer
        if chunk:
            self._recv_buffer.append(BytesBuffer(chunk))

        # read wire regarding corun/hosting mode and flow ctrl
        self._read_wire()

    def _land_one(self):
        while True:
            if self._recv_buffer is None:
                raise WireError('wire disconnected')

            if self._recv_buffer.nbytes <= 0:
                return None  # no single full packet can be read from buffer
            chunk = self._recv_buffer.popleft()
            if not chunk:
                continue
            while True:
                if self._bdy_buf is None:
                    # packet header not fully received yet
                    pe_pos = chunk.find(PACK_END)
                    if pe_pos < 0:
                        # still not enough for packet header
                        if len(chunk) + self._hdr_got >= PACK_HEADER_MAX:
                            try:
                                raise WireError('No packet header within first {} bytes.'.format(
                                    len(chunk) + self._hdr_got)
                                )
                            except WireError as exc:
                                self.disconnect(exc)
                            return
                        hdr_got = self._hdr_got + len(chunk)
                        self._hdr_buf[self._hdr_got:hdr_got] = chunk.data()
                        self._hdr_got = hdr_got
                        break  # proceed to next chunk in buffer
                    hdr_got = self._hdr_got + pe_pos
                    self._hdr_buf[self._hdr_got:hdr_got] = chunk.data(0, pe_pos)
                    self._hdr_got = hdr_got
                    chunk.consume(pe_pos + 1)
                    header_pl = self._hdr_buf[:self._hdr_got]
                    if not header_pl.startswith(PACK_BEGIN):
                        try:
                            raise WireError('Invalid packet start in header: [{}]'.format(header_pl))
                        except WireError as exc:
                            self.disconnect(exc)
                        return
                    ple_pos = header_pl.find(PACK_LEN_END, len(PACK_BEGIN))
                    if ple_pos <= 0:
                        try:
                            raise WireError('No packet length in header: [{}]'.format(header_pl))
                        except WireError as exc:
                            self.disconnect(exc)
                        return
                    pack_len = int(header_pl[len(PACK_BEGIN):ple_pos])
                    self._wire_dir = header_pl[ple_pos + 1:].decode('utf-8')
                    self._hdr_got = 0
                    self._bdy_buf = bytearray(pack_len)
                    self._bdy_got = 0
                else:
                    # packet body not fully received yet
                    needs = len(self._bdy_buf) - self._bdy_got
                    if len(chunk) < needs:
                        # still not enough for packet body
                        bdy_got = self._bdy_got + len(chunk)
                        self._bdy_buf[self._bdy_got:bdy_got] = chunk.data()
                        self._bdy_got = bdy_got
                        break  # proceed to next chunk in buffer
                    # body can be filled now
                    self._bdy_buf[self._bdy_got:] = chunk.data(0, needs)
                    if len(chunk) > needs:  # the other case is equal, means exactly consumed
                        # put back extra data to buffer
                        self._recv_buffer.appendleft(chunk.consume(needs))
                    payload = self._bdy_buf.decode('utf-8')
                    self._bdy_buf = None
                    self._bdy_got = 0
                    wire_dir = self._wire_dir
                    self._wire_dir = None
                    return self.land(payload, wire_dir)
