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

    def __init__(self, server_end=False):
        self._wire = None
        self._conn_fut = (
            None if server_end else asyncio.get_running_loop().create_future()
        )
        self._disc_fut = None
        self._send_mutex = SendCtrl()
        self._co_mutex = asyncio.Lock()
        self.co = None

    async def disconnect(self, err_reason=None, try_send_peer_err=True):
        disc_fut = self._disc_fut
        if disc_fut is not None:
            if err_reason is not None:
                logger.error(
                    rf"""
HBI retry disconnecting {self.net_ident} due to error:
{err_reason}
""",
                    stack_info=True,
                )
            return await disc_fut

        disc_fut = self._disc_fut = asyncio.get_running_loop().create_future()
        if err_reason is not None:
            logger.error(
                rf"""
HBI disconnecting {self.net_ident} due to error:
{err_reason}
""",
                stack_info=True,
            )

        disconn_cb = self.context.get("hbi_disconnecting", None)
        if disconn_cb is not None:
            try:
                maybe_coro = disconn_cb(err_reason)
                if inspect.isawaitable(maybe_coro):
                    await maybe_coro
            except Exception:
                logger.warning(
                    f"HBI disconnecting callback failure ignored.", exc_info=True
                )

        if err_reason is not None and try_send_peer_err:
            if self._wire.transport.is_closing():
                logger.warning(
                    f"Not sending peer error {err_reason!s} as transport is closing.",
                    exc_info=True,
                )
            else:
                # try send peer error
                try:
                    await self._send_text(str(err_reason), b"err")
                except Exception:
                    logger.warning("HBI failed sending peer error", exc_info=True)
                # close outgoing channel
                try:
                    _wire.transport.write_eof()
                except Exception:
                    logger.warning("HBI failed flushing peer error", exc_info=True)

        try:
            _wire.transport.close()
        except OSError:
            # may already be invalid
            pass
        # connection_lost will be called by asyncio loop after out-going packets flushed

        await disc_fut

    def _disconnected(self, exc=None):
        if exc is not None:
            logger.warning(f"HBI connection unwired due to error: {exc}")

        conn_fut = self._conn_fut
        if conn_fut is not None:
            if not conn_fut.done():
                conn_fut.set_exception(exc or RuntimeError("HBI disconnected"))

        self._send_mutex.shutdown(exc)

        disc_fut = self._disc_fut
        if disc_fut is None:
            disc_fut = self._disc_fut = self._loop.create_future()
        elif not disc_fut.done():
            disc_fut.set_result(exc)

    def _connected(self):
        self._send_mutex.startup()

        conn_fut = self._conn_fut
        if conn_fut is not None:  # be None for server connection
            if conn_fut.done():
                assert fut.exception() is None and fut.result() is None, "?!"
            else:
                conn_fut.set_result(None)
