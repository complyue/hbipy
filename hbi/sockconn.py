import asyncio
import json
import logging
import socket
from typing import *

from .conn import *
from .proto import *

__all__ = [
    'HBIC',
]

logger = logging.getLogger(__name__)


class SocketProtocol(asyncio.Protocol):
    """
    HBI protocol over TCP/SSL transports

    """

    __slots__ = (
        'hbic',
        'transport',
        '_recv_paused', '_hdr_buf', '_hdr_got', '_bdy_buf', 'bdy_got', '_wire_dir'
    )

    def __init__(self, hbic):
        self.hbic = hbic
        self.transport = None
        self._recv_paused = False

        self._hdr_buf = bytearray(PACK_HEADER_MAX)
        self._hdr_got = 0
        self._bdy_buf = None
        self._bdy_got = 0
        self._wire_dir = None

    def connection_made(self, transport):
        transport.set_write_buffer_limits(
            self.hbic.high_water_mark_send, self.hbic.low_water_mark_send
        )

        self.transport = transport

        self.hbic._wire = self
        wire_fut = self.hbic._wire_fut
        if wire_fut is not None:
            if not wire_fut.done():
                wire_fut.set_result(self)
            # clear wire future
            self.hbic._wire_fut = None

        self.hbic._connected()

    def pause_writing(self):
        self.hbic._send_mutex.obstruct()

    def resume_writing(self):
        self.hbic._send_mutex.unleash()

    def data_received(self, chunk):
        self.hbic._data_received(chunk)

    def eof_received(self):
        if True is self.hbic._peer_eof():
            return True

        if self.hbic.send_only:
            # if this hbic is send-only, don't let the transport close itself on peer eof
            return True

    def connection_lost(self, exc):
        self.hbic._disconnected(exc)

        self.hbic._wire = None
        self.hbic._wire_fut = None


class HBIC(AbstractHBIC):
    """
    Socket based HBI Connection

    """

    __slots__ = ()

    @classmethod
    def create_server(cls, context_factory, addr, net_opts=None, *, loop=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        if isinstance(addr, (str, bytes)):
            # UNIX domain socket
            return loop.create_unix_server(
                lambda: SocketProtocol(cls(context_factory(), loop=loop, **kwargs)),
                path=addr, **kwargs
            )
        else:
            # TCP socket
            addr_family = socket.AF_INET  # default to IPv4 only
            if net_opts is not None:
                addr_family = net_opts.get('family', addr_family)
            return loop.create_server(
                lambda: SocketProtocol(cls(context_factory(), loop=loop, **kwargs)),
                host=addr.get('host', None), port=addr['port'], **(net_opts or {}),
                family=addr_family, **kwargs
            )

    def get_remote_host(self):
        peername = self._wire.transport.get_extra_info('peername')
        if len(peername) in (2, 4):
            return peername[0]
        raise NotImplementedError('Socket transport other than tcp4/tcp6 not supported yet.')

    def get_remote_port(self):
        peername = self._wire.transport.get_extra_info('peername')
        if len(peername) in (2, 4):
            return peername[1]
        raise NotImplementedError('Socket transport other than tcp4/tcp6 not supported yet.')

    def get_local_host(self):
        sockname = self._wire.transport.get_extra_info('sockname')
        if len(sockname) in (2, 4):
            return sockname[0]
        raise NotImplementedError('Socket transport other than tcp4/tcp6 not supported yet.')

    def get_local_port(self):
        sockname = self._wire.transport.get_extra_info('sockname')
        if len(sockname) in (2, 4):
            return sockname[1]
        raise NotImplementedError('Socket transport other than tcp4/tcp6 not supported yet.')

    @property
    def net_info(self):
        _wire = self._wire
        if _wire is None:
            return '<unwired>'
        try:
            transport = _wire.transport
            if transport.is_closing():
                return '<closing>'
            return f"[{transport.get_extra_info('sockname')} <=> {transport.get_extra_info('peername')}]"
        except Exception as exc:
            return f'<HBI bogon wire: {exc}>'

    async def _connect(self):
        assert self.addr, 'no addr has been specified ?!'

        if self._wire is not None:
            raise asyncio.InvalidStateError(
                'requesting new connection with transport already wired')

        wire_fut = self._wire_fut
        if wire_fut is None or wire_fut.done():
            self._wire_fut = self._loop.create_future()
        else:
            # already attempting wiring, do not repeat
            return

        try:

            if isinstance(self.addr, (str, bytes)):
                # UNIX domain socket connection
                transport, protocol = await self._loop.create_unix_connection(
                    lambda: SocketProtocol(self),
                    self.addr,
                )
            else:
                # TCP socket
                addr_family = socket.AF_INET  # default to IPv4 only
                if self.net_opts is not None:
                    addr_family = self.net_opts.get('family', addr_family)
                transport, protocol = await self._loop.create_connection(
                    lambda: SocketProtocol(self),
                    self.addr['host'], self.addr['port'],
                    **self.net_opts or {}, family=addr_family,
                )

            assert protocol is self._wire and transport is protocol.transport, 'conn not made atm ?!'

        except Exception as exc:
            fut = self._conn_fut
            if fut is not None:
                self._conn_fut = None
                fut.set_exception(exc)

    async def _send_text(self, code, wire_dir=b''):
        if isinstance(code, bytes):
            payload = code
        elif isinstance(code, str):
            payload = code.encode('utf-8')
        else:
            # try convert to json and send
            payload = json.dumps(code).encode('utf-8')

        await self._send_mutex.flowing()
        self._wire.transport.writelines([
            b'[%d#%s]' % (len(payload), wire_dir),
            payload
        ])

    async def _send_buffer(self, buf):
        # wait sendable for each single buffer
        await self._send_mutex.flowing()
        self._wire.transport.write(buf)

    def _pause_recv(self):
        if self._wire._recv_paused:
            return

        self._wire.transport.pause_reading()
        self._wire._recv_paused = True

    def _resume_recv(self):
        if not self._wire._recv_paused:
            return

        self._wire.transport.resume_reading()
        self._wire._recv_paused = False

    def _cut_wire(self, err_reason=None, err_stack=None):
        _wire = self._wire
        if _wire is None:  # already closed
            return
        _wire.transport.close()
        self._wire = None
        # connection_lost will be called afterwards

    async def _disconnect(self, err_reason=None, err_stack=None, try_send_peer_err=True):
        _wire = self._wire
        if _wire is None:  # already closed
            return

        if err_reason is not None and try_send_peer_err:
            if not _wire.transport.is_closing():
                # try send peer error
                try:
                    await self._send_text(rf'''
handlePeerErr({err_reason!r},{err_stack!r})
''', b'wire')
                except Exception:
                    logger.warning('HBI failed sending peer error', exc_info=True)
                # close outgoing channel
                try:
                    _wire.transport.write_eof()
                except Exception:
                    logger.warning('HBI failed flushing peer error', exc_info=True)

        # clear this only after peer error has been sent out
        self._wire = None

        try:
            _wire.transport.close()
        except OSError:
            # may already be invalid
            pass
        # connection_lost will be called by asyncio loop after out-going packets flushed

    def _land_one(self) -> Optional[tuple]:

        while True:
            if self._recv_buffer is None:
                raise RuntimeError('HBI wire disconnected')

            if self._recv_buffer.nbytes <= 0:
                return None  # no single full packet can be read from buffer
            chunk = self._recv_buffer.popleft()
            if not chunk:
                continue
            while True:
                if self._wire._bdy_buf is None:
                    # packet header not fully received yet
                    pe_pos = chunk.find(PACK_END)
                    if pe_pos < 0:
                        # still not enough for packet header
                        if len(chunk) + self._wire._hdr_got >= PACK_HEADER_MAX:
                            raise RuntimeError(
                                f'No packet header within first {len(chunk) + self._wire._hdr_got} bytes.'
                            )
                        hdr_got = self._wire._hdr_got + len(chunk)
                        self._wire._hdr_buf[self._wire._hdr_got:hdr_got] = chunk.data()
                        self._wire._hdr_got = hdr_got
                        break  # proceed to next chunk in buffer
                    hdr_got = self._wire._hdr_got + pe_pos
                    self._wire._hdr_buf[self._wire._hdr_got:hdr_got] = chunk.data(0, pe_pos)
                    self._wire._hdr_got = hdr_got
                    chunk.consume(pe_pos + 1)
                    header_pl = self._wire._hdr_buf[:self._wire._hdr_got]
                    if not header_pl.startswith(PACK_BEGIN):
                        rpt_len = len(header_pl)
                        rpt_hdr = header_pl[:min(self._wire._hdr_got, 30)]
                        rpt_net = self.net_info
                        raise RuntimeError(
                            f'Invalid packet start in header: len: {rpt_len}, peer: {rpt_net}, head: [{rpt_hdr}]'
                        )
                    ple_pos = header_pl.find(PACK_LEN_END, len(PACK_BEGIN))
                    if ple_pos <= 0:
                        raise RuntimeError(f'No packet length in header: [{header_pl}]')
                    pack_len = int(header_pl[len(PACK_BEGIN):ple_pos])
                    self._wire._wire_dir = header_pl[ple_pos + 1:].decode('utf-8')
                    self._wire._hdr_got = 0
                    self._wire._bdy_buf = bytearray(pack_len)
                    self._wire._bdy_got = 0
                else:
                    # packet body not fully received yet
                    needs = len(self._wire._bdy_buf) - self._wire._bdy_got
                    if len(chunk) < needs:
                        # still not enough for packet body
                        bdy_got = self._wire._bdy_got + len(chunk)
                        self._wire._bdy_buf[self._wire._bdy_got:bdy_got] = chunk.data()
                        self._wire._bdy_got = bdy_got
                        break  # proceed to next chunk in buffer
                    # body can be filled now
                    self._wire._bdy_buf[self._wire._bdy_got:] = chunk.data(0, needs)
                    if len(chunk) > needs:  # the other case is equal, means exactly consumed
                        # put back extra data to buffer
                        self._recv_buffer.appendleft(chunk.consume(needs))
                    payload = self._wire._bdy_buf.decode('utf-8')
                    self._wire._bdy_buf = None
                    self._wire._bdy_got = 0
                    wire_dir = self._wire._wire_dir
                    self._wire._wire_dir = None
                    return self._land_(payload, wire_dir)
