import asyncio
import socket
from typing import *

from ._details import *
from .ho import *
from .log import *
from .po import *

__all__ = ["HBIC", "HBIS"]

logger = get_logger(__name__)


class HBIC:
    """
    HBI client over a socket

    """

    def __init__(
        self,
        addr,
        ctx,
        *,
        app_queue_size: int = 200,
        wire_buf_high=50 * 1024 * 1024,
        wire_buf_low=10 * 1024 * 1024,
        net_opts: Optional[dict] = None,
    ):
        self.addr = addr
        self.ctx = ctx

        self.app_queue_size = app_queue_size
        self.wire_buf_high = wire_buf_high
        self.wire_buf_low = wire_buf_low
        self.net_opts = net_opts if net_opts is not None else {}

        if isinstance(self.addr, (str, bytes)):
            # UNIX domain socket
            pass
        else:  # TCP socket
            if "family" not in self.net_opts:
                # default to IPv4 only
                self.net_opts["family"] = socket.AF_INET

        self._wire = None

    async def __aenter__(self):
        wire = await self.connect()

        return wire.po

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            err_reason = None
        else:
            import traceback

            err_msg = str(exc_type) + ":" + str(exc_val)
            err_stack = "".join(traceback.format_exception(exc_type, exc_val, exc_tb))
            err_reason = err_msg + "\n" + err_stack

        await self.disconnect(err_reason, try_send_peer_err=True)

    async def connect(self):
        wire = self._wire
        if wire is not None:
            if wire.connected:
                return wire
            wire = None

        loop = asyncio.get_running_loop()

        def ProtocolFactory():
            po = PostingEnd()
            if self.ctx is None:  # posting only
                ho = None
            else:
                ho = HostingEnd(po, self.app_queue_size)
                ho.ctx = self.ctx
            return SocketWire(po, ho, self.wire_buf_high, self.wire_buf_low)

        if isinstance(self.addr, (str, bytes)):
            # UNIX domain socket
            transport, wire = await loop.create_unix_connection(
                ProtocolFactory, path=self.addr, **self.net_opts
            )
        else:
            # TCP socket
            transport, wire = await loop.create_connection(
                ProtocolFactory,
                host=addr.get("host", "127.0.0.1"),
                port=addr.get("port", 3232),
                **self.net_opts,
            )
        self._wire = wire

        return wire

    async def disconnect(self, err_reason=None, try_send_peer_err=True):
        wire = self._wire
        if wire is not None:
            if wire.connected:
                await wire.disconnect(err_reason, try_send_peer_err)

    def __str__(self):
        wire = self._wire
        if wire is not None:
            return f"[HBIC#{self.addr!s}@{wire.net_ident!s}]"
        return f"[HBIC#{self.addr!s}]"


class HBIS:
    """
    HBI server over sockets

    """

    def __init__(
        self,
        addr,
        context_factory,
        *,
        app_queue_size: int = 100,
        wire_buf_high=20 * 1024 * 1024,
        wire_buf_low=6 * 1024 * 1024,
        net_opts: Optional[dict] = None,
    ):
        self.addr = addr
        self.context_factory = context_factory

        self.app_queue_size = app_queue_size
        self.wire_buf_high = wire_buf_high
        self.wire_buf_low = wire_buf_low
        self.net_opts = net_opts if net_opts is not None else {}

        if isinstance(self.addr, (str, bytes)):
            # UNIX domain socket
            pass
        else:  # TCP socket
            if "family" not in self.net_opts:
                # default to IPv4 only
                self.net_opts["family"] = socket.AF_INET

        self._server = None

    async def server(self):
        if self._server is not None:
            return self._server

        loop = asyncio.get_running_loop()

        def ProtocolFactory():
            po = PostingEnd()
            ho = HostingEnd(po, self.app_queue_size)
            ho.ctx = self.context_factory(po=po, ho=ho)
            return SocketWire(po, ho, self.wire_buf_high, self.wire_buf_low)

        if isinstance(self.addr, (str, bytes)):
            # UNIX domain socket
            self._server = await loop.create_unix_server(
                ProtocolFactory, path=self.addr, **self.net_opts
            )
        else:
            # TCP socket
            self._server = await loop.create_server(
                ProtocolFactory,
                host=addr.get("host", "127.0.0.1"),
                port=addr.get("port", 3232),
                **self.net_opts,
            )

        return self._server

    async def serve_until_closed(self):
        server = await self.server()
        await server.wait_closed()

    def __str__(self):
        server = self._server
        if server is not None:
            return f"[HBIS#{self.addr!s}@{server.get_extra_info('sockname')!s}]"
        return f"[HBIS#{self.addr!s}]"
