import asyncio

from ..log import *
from ..proto import *

__all__ = ["SocketWire"]

logger = get_logger(__name__)


class SocketWire(asyncio.Protocol):
    """
    HBI protocol over TCP/SSL transports

    """

    __slots__ = (
        "po",
        "ho",
        "transport",
        "_recv_paused",
        "_hdr_buf",
        "_hdr_got",
        "_bdy_buf",
        "bdy_got",
        "_wire_dir",
    )

    def __init__(self, po, ho):
        self.po = po
        self.ho = ho

        self.transport = None

        self._recv_paused = False
        self._hdr_buf = bytearray(PACK_HEADER_MAX)
        self._hdr_got = 0
        self._bdy_buf = None
        self._bdy_got = 0
        self._wire_dir = None

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
        ho._data_received(chunk)

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
