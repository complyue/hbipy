import asyncio
from typing import *

from .log import *

__all__ = ["Conver"]

logger = get_logger(__name__)


class Conver:
    """
    Abstract Conversation

    """

    __slots__ = ()

    @property
    def coid(self) -> str:
        raise NotImplementedError

    async def send_code(self, code):
        raise NotImplementedError

    async def send_obj(self, code):
        raise NotImplementedError

    async def send_data(self, bufs):
        raise NotImplementedError

    async def recv_obj(self):
        raise NotImplementedError

    async def recv_data(self, bufs):
        raise NotImplementedError

    def is_ended(self):
        raise NotImplementedError

    async def wait_ended():
        await self._send_done_fut


class HoCo(Conver):
    """
    Hosting Conversation

    """

    __slots__ = ("ho", "_coid", "_send_done_fut")

    def __init__(self, ho, coid):
        self.ho = ho
        self._coid = coid
        self._send_done_fut = asyncio.get_running_loop().create_future()

    @property
    def coid(self) -> str:
        return self._coid

    async def send_code(self, code):
        ho = self.ho
        if self is not ho.co:
            raise asyncio.InvalidStateError("Hosting conversation ended already!")
        po = ho.po

        await po._send_code(code)

    async def send_obj(self, code):
        ho = self.ho
        if self is not ho.co:
            raise asyncio.InvalidStateError("Hosting conversation ended already!")
        po = ho.po

        await po._send_code(code, b"co_recv")

    async def send_data(self, bufs):
        ho = self.ho
        if self is not ho.co:
            raise asyncio.InvalidStateError("Hosting conversation ended already!")
        po = ho.po

        await po._send_data(bufs)

    async def recv_obj(self):
        ho = self.ho
        if self is not ho.co:
            raise asyncio.InvalidStateError("Hosting conversation ended already!")

        return await ho._recv_obj()

    async def recv_data(self, bufs):
        ho = self.ho
        if self is not ho.co:
            raise asyncio.InvalidStateError("Hosting conversation ended already!")

        await ho._recv_data(bufs)

    def is_ended(self):
        return self._send_done_fut.done()

    async def wait_ended():
        await self._send_done_fut


class PoCo(Conver):
    """
    Posting Conversation

    """

    __slots__ = ("po", "_send_done_fut", "_begin_acked_fut", "_end_acked_fut")

    def __init__(self, po):
        self.po = po

        self._send_done_fut = asyncio.get_running_loop().create_future()
        self._begin_acked_fut = None
        self._end_acked_fut = None

    @property
    def coid(self) -> str:
        return repr(id(self))

    async def __aenter__(self):
        await self.begin()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        po = self.po
        if po._wire.connected:
            await self.end()

    async def begin(self):
        if self._begin_acked_fut is not None:
            raise asyncio.InvalidStateError("co_begin sent already!")

        po = self.po
        coq = po._coq
        while coq:
            tail_co = coq[-1]
            if tail_co.is_ended():
                break
            await tail_co.wait_ended()

        self._begin_acked_fut = asyncio.get_running_loop().create_future()

        coq.append(self)

        try:
            await po._send_text(self.coid, b"co_begin")
        except Exception as exc:
            err_msg = "Error sending co_begin: " + str(exc)
            err_stack = "".join(traceback.format_exc())
            err_reason = err_msg + "\n" + err_stack
            po.disconnect(err_reason)
            raise

    async def end(self):
        if self._begin_acked_fut is None:
            raise asyncio.InvalidStateError("co_begin not sent yet!")
        if self._end_acked_fut is not None:
            raise asyncio.InvalidStateError("co_end sent already!")

        po = self.po
        assert self is po._coq[-1], "co not tail of po's coq ?!"

        self._send_done_fut.set_result(self.coid)

        self._end_acked_fut = asyncio.get_running_loop().create_future()

        try:
            await po._send_text(self.coid, b"co_end")
        except Exception as exc:
            err_msg = "Error sending co_end: " + str(exc)
            err_stack = "".join(traceback.format_exc())
            err_reason = err_msg + "\n" + err_stack
            po.disconnect(err_reason)
            raise

    def is_ended(self):
        return self._send_done_fut.done()

    async def wait_ended():
        await self._send_done_fut

    def _begin_acked(self, coid):
        if self.coid != coid:
            raise asyncio.InvalidStateError("coid mismatch ?!")

        fut = self._begin_acked_fut
        if fut is None:
            raise asyncio.InvalidStateError("co_begin not sent yet!")
        ended = self._end_acked_fut
        if ended is not None and ended.done():
            raise asyncio.InvalidStateError("co_end acked already!")

        fut.set_result(coid)

    def _end_acked(self, coid):
        if self.coid != coid:
            raise asyncio.InvalidStateError("coid mismatch ?!")

        fut = self._end_acked_fut
        if fut is None:
            raise asyncio.InvalidStateError("co_end not sent yet!")

        fut.set_result(coid)

    async def response_begin(self):
        fut = self._begin_acked_fut
        if fut is None:
            raise asyncio.InvalidStateError("co_begin not sent yet!")
        await fut

    async def response_end(self):
        fut = self._end_acked_fut
        if fut is None:
            raise asyncio.InvalidStateError("co_end not sent yet!")
        await fut

    async def send_code(self, code):
        if self._begin_acked_fut is None:
            raise asyncio.InvalidStateError("co_begin not sent yet!")

        po = self.po
        assert self is po._coq[-1], "co not tail of po's coq ?!"

        await po._send_code(code)

    async def send_obj(self, code):
        if self._begin_acked_fut is None:
            raise asyncio.InvalidStateError("co_begin not sent yet!")

        po = self.po
        assert self is po._coq[-1], "co not tail of po's coq ?!"

        await po._send_code(code, b"co_recv")

    async def send_data(self, bufs):
        if self._begin_acked_fut is None:
            raise asyncio.InvalidStateError("co_begin not sent yet!")

        po = self.po
        assert self is po._coq[-1], "co not tail of po's coq ?!"

        await po._send_data(bufs)

    async def recv_obj(self):
        if self._begin_acked_fut is None:
            raise asyncio.InvalidStateError("co_begin not sent yet!")
        await self.response_begin()

        po = self.po
        assert self is po._coq[0], "co not head of po's coq ?!"

        return await po._wire.ho._recv_obj()

    async def recv_data(self, bufs):
        if self._begin_acked_fut is None:
            raise asyncio.InvalidStateError("co_begin not sent yet!")
        await self.response_begin()

        po = self.po
        assert self is po._coq[0], "co not head of po's coq ?!"

        await po._wire.ho._recv_data(bufs)

    async def get_obj(self, code):
        if self._begin_acked_fut is None:
            raise asyncio.InvalidStateError("co_begin not sent yet!")

        po = self.po
        assert self is po._coq[0], "co not head of po's coq ?!"
        await po._send_text(code, b"co_send")

        return await self.recv_obj()
