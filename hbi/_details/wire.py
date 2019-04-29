import asyncio

from ..log import *
from ..proto import *
from .buflist import *
from .bytesbuf import *

__all__ = ["SocketWire"]

logger = get_logger(__name__)


class SocketWire(asyncio.Protocol):
    """
    HBI protocol over TCP/SSL transports

    """

    __slots__ = (
        "po",
        "ho",
        "wire_buf_high",
        "wire_buf_low",
        "transport",
        "_hdr_buf",
        "_hdr_got",
        "_bdy_buf",
        "bdy_got",
        "_wire_dir",
        "_recv_buffer",
        "_data_sink",
    )

    def __init__(
        self, po, ho, wire_buf_high=20 * 1024 * 1024, wire_buf_low=6 * 1024 * 1024
    ):
        self.po = po
        self.ho = ho
        self.wire_buf_high = wire_buf_high
        self.wire_buf_low = wire_buf_low

        self.transport = None

        self._hdr_buf = bytearray(PACK_HEADER_MAX)
        self._hdr_got = 0
        self._bdy_buf = None
        self._bdy_got = 0
        self._wire_dir = None

        # for packet parsing and data pumping
        self._recv_buffer = BufferList()
        self._data_sink = None

        if po is not None:
            po._wire = self
        if ho is not None:
            ho._wire = self

    def connection_made(self, transport):
        self.transport = transport
        net_ident = self.net_ident

        po = self.po
        if po is not None:
            assert po._wire is self, "wire mismatch ?!"
            po._connected(net_ident)

        ho = self.ho
        if ho is not None:
            assert ho._wire is self, "wire mismatch ?!"
            ho._connected(net_ident)

    def data_received(self, chunk):
        ho = self.ho
        if ho is None:
            raise RuntimeError(f"Posting only connection received data ?!")
        assert ho._wire is self, "wire mismatch ?!"
        self._data_received(chunk)

    def eof_received(self):
        ho = self.ho
        if ho is None:  # posting only connection
            return True  # don't let the transport close itself on peer eof
        # hosting endpoint can prevent the transport from being closed by returning True
        return ho._peer_eof()

    def connection_lost(self, exc):
        po = self.po
        if po is not None:
            assert po._wire is self, "wire mismatch ?!"
            po._disconnected(exc)
        ho = self.ho
        if ho is not None:
            assert ho._wire is self, "wire mismatch ?!"
            ho._disconnected(exc)

    def pause_writing(self):
        self.po._send_mutex.obstruct()

    def resume_writing(self):
        self.po._send_mutex.unleash()

    @property
    def connected(self):
        transport = self.transport
        if transport is None:
            return False
        return not transport.is_closing()

    async def disconnect(self, err_reason=None, try_send_peer_err=True):
        po, ho = self.po, self.ho
        if ho is not None:
            # hosting endpoint closes posting endpoint on its own closing
            await ho.disconnect(err_reason, try_send_peer_err)
        elif po is not None:
            # a posting only wire
            await po.disconnect(err_reason, try_send_peer_err)
        else:
            assert False, "neither po nor ho ?!"

    @property
    def net_ident(self):
        transport = self.transport
        if transport is None:
            return "<unwired>"

        try:
            addr_info = f"{transport.get_extra_info('sockname')}<=>{transport.get_extra_info('peername')}"
            if transport.is_closing():
                return f"@closing@{addr_info}"
            return addr_info
        except Exception as exc:
            return f"<HBI bogon wire: {exc!s}>"

    @property
    def remote_host(self):
        transport = self.transport
        if transport is None:
            raise asyncio.InvalidStateError("Socket not wired!")

        peername = transport.get_extra_info("peername")
        if len(peername) in (2, 4):
            return peername[0]
        raise NotImplementedError(
            "Socket transport other than tcp4/tcp6 not supported yet."
        )

    @property
    def remote_port(self):
        transport = self.transport
        if transport is None:
            raise asyncio.InvalidStateError("Socket not wired!")

        peername = transport.get_extra_info("peername")
        if len(peername) in (2, 4):
            return peername[1]
        raise NotImplementedError(
            "Socket transport other than tcp4/tcp6 not supported yet."
        )

    @property
    def local_host(self):
        transport = self.transport
        if transport is None:
            raise asyncio.InvalidStateError("Socket not wired!")

        sockname = transport.get_extra_info("sockname")
        if len(sockname) in (2, 4):
            return sockname[0]
        raise NotImplementedError(
            "Socket transport other than tcp4/tcp6 not supported yet."
        )

    @property
    def local_port(self):
        transport = self.transport
        if transport is None:
            raise asyncio.InvalidStateError("Socket not wired!")

        sockname = transport.get_extra_info("sockname")
        if len(sockname) in (2, 4):
            return sockname[1]
        raise NotImplementedError(
            "Socket transport other than tcp4/tcp6 not supported yet."
        )

    def _data_received(self, chunk):
        # push to buffer
        if chunk:
            self._recv_buffer.append(BytesBuffer(chunk))

        while True:  # consume as much data as possible from wire

            # feed as much buffered data as possible to data sink if present
            while self._data_sink:
                # make sure data keep flowing in regarding lwm
                if self._recv_buffer.nbytes <= self.wire_buf_low:
                    self.transport.resume_reading()

                if self._recv_buffer is None:
                    # unexpected disconnect
                    self._data_sink(None)
                    return

                chunk = self._recv_buffer.popleft()
                if not chunk:
                    # no more buffered data, wire is empty, return
                    return
                self._data_sink(chunk)

            while True:
                if self._disc_fut is not None:
                    return

                # ctrl incoming flow regarding hwm/lwm
                buffered_amount = self._recv_buffer.nbytes
                if buffered_amount >= self.wire_buf_high:
                    self.transport.pause_reading()
                elif buffered_amount <= self.wire_buf_low:
                    self.transport.resume_reading()

                # land any packet available from wire
                if not self._land_one():
                    # no more packet to land
                    return

    def _land_one(self) -> bool:
        while True:
            if self._recv_buffer.nbytes <= 0:
                return False  # no single full packet can be read from buffer

            chunk = self._recv_buffer.popleft()
            if not chunk:
                continue

            while True:
                if _www_._bdy_buf is None:
                    # packet header not fully received yet
                    pe_pos = chunk.find(b"]")
                    if pe_pos < 0:
                        # still not enough for packet header
                        if len(chunk) + _www_._hdr_got >= PACK_HEADER_MAX:
                            raise RuntimeError(
                                f"No packet header within first {len(chunk) + _www_._hdr_got} bytes."
                            )
                        hdr_got = _www_._hdr_got + len(chunk)
                        _www_._hdr_buf[_www_._hdr_got : hdr_got] = chunk.data()
                        _www_._hdr_got = hdr_got
                        break  # proceed to next chunk in buffer
                    hdr_got = _www_._hdr_got + pe_pos
                    _www_._hdr_buf[_www_._hdr_got : hdr_got] = chunk.data(0, pe_pos)
                    _www_._hdr_got = hdr_got
                    chunk.consume(pe_pos + 1)
                    header_pl = _www_._hdr_buf[: _www_._hdr_got]
                    if not header_pl.startswith(b"["):
                        rpt_len = len(header_pl)
                        rpt_hdr = header_pl[: min(_www_._hdr_got, 30)]
                        rpt_net = self.net_ident
                        raise RuntimeError(
                            f"Invalid packet start in header: len: {rpt_len}, peer: {rpt_net}, head: [{rpt_hdr}]"
                        )
                    ple_pos = header_pl.find(b"#", 1)
                    if ple_pos <= 0:
                        raise RuntimeError(f"No packet length in header: [{header_pl}]")
                    pack_len = int(header_pl[1:ple_pos])
                    _www_._wire_dir = header_pl[ple_pos + 1 :].decode("utf-8")
                    _www_._hdr_got = 0
                    _www_._bdy_buf = bytearray(pack_len)
                    _www_._bdy_got = 0
                else:
                    # packet body not fully received yet
                    needs = len(_www_._bdy_buf) - _www_._bdy_got
                    if len(chunk) < needs:
                        # still not enough for packet body
                        bdy_got = _www_._bdy_got + len(chunk)
                        _www_._bdy_buf[_www_._bdy_got : bdy_got] = chunk.data()
                        _www_._bdy_got = bdy_got
                        break  # proceed to next chunk in buffer
                    # body can be filled now
                    _www_._bdy_buf[_www_._bdy_got :] = chunk.data(0, needs)
                    if (
                        len(chunk) > needs
                    ):  # the other case is equal, means exactly consumed
                        # put back extra data to buffer
                        self._recv_buffer.appendleft(chunk.consume(needs))
                    payload = _www_._bdy_buf.decode("utf-8")
                    _www_._bdy_buf = None
                    _www_._bdy_got = 0
                    wire_dir = _www_._wire_dir
                    _www_._wire_dir = None

                    self.ho._land_packet(payload, wire_dir)
                    return True

    def _begin_offload(self, sink):
        if self._data_sink is not None:
            raise RuntimeError("HBI already offloading data")
        if not callable(sink):
            raise RuntimeError(
                "HBI sink to offload data must be a function accepting data chunks"
            )
        self._data_sink = sink
        if self._recv_buffer.nbytes > 0:
            # having buffered data, dump to sink
            while self._data_sink is sink:
                chunk = self._recv_buffer.popleft()
                # make sure data keep flowing in regarding lwm
                if self._recv_buffer.nbytes <= self.wire_buf_low:
                    self.transport.resume_reading()
                if not chunk:
                    break
                sink(chunk)
        else:
            sink(b"")

    def _end_offload(self, read_ahead=None, sink=None):
        if sink is not None and sink is not self._data_sink:
            raise RuntimeError("HBI resuming from wrong sink")
        self._data_sink = None
        if read_ahead:
            self._recv_buffer.appendleft(read_ahead)
        # this should have been called from a receiving loop or coroutine,
        # so return here, and the recv buffer kept being processed,
        # or the coroutine proceed
        pass
