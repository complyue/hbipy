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

        po._wire = self
        ho._wire = self

    def connection_made(self, transport):
        self.transport = transport

        po = self.po
        if po is not None:
            assert po._wire is self, "wire mismatch ?!"
            po._connected()
        else:
            logger.warning("No posting endpoint ?!")

        ho = self.ho
        if ho is not None:
            assert ho._wire is self, "wire mismatch ?!"
            ho._connected()
        else:
            pass  # posting only connection

    def pause_writing(self):
        self.po._send_mutex.obstruct()

    def resume_writing(self):
        self.po._send_mutex.unleash()

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
        # hosting endpoint can prevent the transport from closing by returning True
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
