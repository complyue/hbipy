import asyncio
import inspect
from typing import *

from .log import *
from .sendctrl import SendCtrl

__all__ = ["PostingEnd"]

logger = get_logger(__name__)


class PostingEnd:
    """
    HBI posting endpoint

    """

    def __init__(self):
        self._wire = None
        self.net_ident = "<unwired>"

        self._conn_fut = asyncio.get_running_loop().create_future()
        self._disc_fut = None

        self._send_mutex = SendCtrl()
        self._co_mutex = asyncio.Lock()
        self.co = None

    async def connected(self):
        await self._conn_fut

    async def co(self):
        # TODO establish active conversation
        co = ...
        return co

    async def disconnect(self, err_reason=None, try_send_peer_err=True):
        _wire = self._wire
        if _wire is None:
            raise asyncio.InvalidStateError(
                f"HBI {self.net_ident} posting endpoint not wired yet!"
            )

        disc_fut = self._disc_fut
        if disc_fut is not None:
            if err_reason is not None:
                logger.error(
                    rf"""
HBI {self.net_ident} repeated disconnection of due to error:
{err_reason}
""",
                    stack_info=True,
                )
            return await disc_fut

        if err_reason is not None:
            logger.error(
                rf"""
HBI {self.net_ident} disconnecting due to error:
{err_reason}
""",
                stack_info=True,
            )

        disc_fut = self._disc_fut = asyncio.get_running_loop().create_future()

        disconn_cb = self.context.get("hbi_disconnecting", None)
        if disconn_cb is not None:
            try:
                maybe_coro = disconn_cb(err_reason)
                if inspect.isawaitable(maybe_coro):
                    await maybe_coro
            except Exception:
                logger.warning(
                    f"HBI {self.net_ident} disconnecting callback failure ignored.",
                    exc_info=True,
                )

        try:
            if _wire.transport.is_closing():
                logger.warning(
                    f"HBI {self.net_ident} transport already closing.", stack_info=True
                )

                if err_reason is not None and try_send_peer_err:
                    logger.warning(
                        f"Not sending peer error {err_reason!s} as transport is closing.",
                        exc_info=True,
                    )
            else:
                if err_reason is not None and try_send_peer_err:
                    try:
                        await self._send_text(str(err_reason), b"err")
                    except Exception:
                        logger.warning(
                            "HBI {self.net_ident} failed sending peer error",
                            exc_info=True,
                        )

                # close outgoing channel
                # This method can raise NotImplementedError if the transport (e.g. SSL) doesn’t support half-closed connections.
                # TODO handle SSL cases once supported
                _wire.transport.write_eof()

            disc_fut.set_result(None)
        except Exception as exc:
            logger.warning(
                "HBI {self.net_ident} failed closing posting endpoint.", exc_info=True
            )
            disc_fut.set_exception(exc)

        assert disc_fut.done()
        await disc_fut

    async def disconnected(self):
        disc_fut = self._disc_fut
        if disc_fut is None:
            raise asyncio.InvalidStateError("Not disconnecting")
        await disc_fut

    # should be called by wire protocol
    def _connected(self, net_ident):
        self.net_ident = net_ident

        self._send_mutex.startup()

        conn_fut = self._conn_fut
        assert conn_fut is not None, "?!"
        if conn_fut.done():
            assert fut.exception() is None and fut.result() is None, "?!"
        else:
            conn_fut.set_result(None)

    # should be called by wire protocol
    def _disconnected(self, exc=None):
        if exc is not None:
            logger.warning(
                f"HBI {self.net_ident} connection unwired due to error: {exc}"
            )

        conn_fut = self._conn_fut
        if conn_fut is not None and not conn_fut.done():
            conn_fut.set_exception(
                exc
                or asyncio.InvalidStateError(
                    f"HBI {self.net_ident} disconnected due to {exc!s}"
                )
            )

        self._send_mutex.shutdown(exc)

        disc_fut = self._disc_fut
        if disc_fut is None:
            disc_fut = self._disc_fut = asyncio.get_running_loop().create_future()

        if not disc_fut.done():
            disc_fut.set_result(exc)
        elif disc_fut.result() is not exc:
            logger.warning(
                f"HBI {self.net_ident} disconnection exception not reported to application: {exc!s}"
            )
