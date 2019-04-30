import asyncio
from typing import *

from .log import *

__all__ = ["Conver"]

logger = get_logger(__name__)


class Conver:
    """
    HBI active conversation at posting end.

    """

    def __init__(self, po):
        self._po = po

        self._end_sent_fut = asyncio.get_running_loop().create_future()
        self._begin_acked_fut = None
        self._end_acked_fut = None

    async def response_begin(self):
        fut = self._begin_acked_fut
        if fut is None:
            raise asyncio.InvalidStateError("co_begin not yet sent!")
        ended = self._end_acked_fut
        if ended is not None and ended.done():
            raise asyncio.InvalidStateError("co_end already acked")
        await fut

    async def response_end(self):
        fut = self._end_acked_fut
        if fut is None:
            raise asyncio.InvalidStateError("co_end not sent!")
        await fut

    async def begin(self):
        if self._begin_acked_fut is not None:
            raise asyncio.InvalidStateError("co_begin already sent!")

        po = self._po
        if po._coq:
            last_co = po._coq[-1]
            await last_co._end_sent_fut

        self._begin_acked_fut = asyncio.get_running_loop().create_future()

        po._coq.append(self)

        try:
            await po._send_text(repr(id(self)), b"co_begin")
        except Exception as exc:
            logger.error("Error sending co_begin.", exc_info=True)
            self._begin_acked_fut.set_exception(exc)

    def _begin_acked(self, coid):
        if repr(id(self)) != coid:
            raise asyncio.InvalidStateError("coid mismatch ?!")
        fut = self._begin_acked_fut
        if fut is None:
            raise asyncio.InvalidStateError("co_begin not yet sent!")
        ended = self._end_acked_fut
        if ended is not None and ended.done():
            raise asyncio.InvalidStateError("co_end already acked")

        fut.set_result(coid)

    def _end_acked(self, coid):
        if repr(id(self)) != coid:
            raise asyncio.InvalidStateError("coid mismatch ?!")
        fut = self._end_acked_fut
        if fut is None:
            raise asyncio.InvalidStateError("co_end not yet sent!")

        fut.set_result(coid)

    async def send_code(self, code):
        if self._begin_acked_fut is None:
            raise asyncio.InvalidStateError("co_begin not yet sent!")
        po = self._po
        assert self in po._coq, "co not in po's coq ?!"
        await po._send_code(code)

    async def send_data(self, bufs):
        if self._begin_acked_fut is None:
            raise asyncio.InvalidStateError("co_begin not yet sent!")
        po = self._po
        assert self in po._coq, "co not in po's coq ?!"
        await po._send_data(bufs)

    async def recv_obj(self):
        if self._begin_acked_fut is None:
            raise asyncio.InvalidStateError("co_begin not yet sent!")
        await self.response_begin()

        return await self._po._wire.ho._recv_obj()

    async def recv_data(self, bufs):
        if self._begin_acked_fut is None:
            raise asyncio.InvalidStateError("co_begin not yet sent!")
        await self.response_begin()

        await self._po._wire.ho._recv_data(bufs)

    async def get_obj(self, code):
        if self._begin_acked_fut is None:
            raise asyncio.InvalidStateError("co_begin not yet sent!")
        po = self._po
        assert self in po._coq, "co not in po's coq ?!"
        await po._send_text(code, b"co_send")
        return await self.recv_obj()

    async def end(self):
        if self._end_acked_fut is not None:
            raise asyncio.InvalidStateError("co_end already sent!")

        po = self._po
        assert self is po._coq[-1], "open co not the tail of po's coq ?!"

        self._end_acked_fut = asyncio.get_running_loop().create_future()

        try:
            await po._send_text(repr(id(self)), b"co_end")
        except Exception as exc:
            logger.error("Error sending co_end.", exc_info=True)
            self._end_acked_fut.set_exception(exc)

    async def __aenter__(self):
        await self.begin()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        po = self._po
        if po._wire.connected:
            await self.end()
