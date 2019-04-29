import asyncio
import inspect
from collections import deque
from typing import *

from ._details import *
from .buflist import *
from .bytesbuf import *
from .context import run_in_context
from .log import *

__all__ = ["HostingEnd"]

logger = get_logger(__name__)


class HostingEnd:
    """
    HBI hosting endpoint

    """

    def __init__(self, po):
        self.po = po

        self._wire = None
        self.net_ident = "<unwired>"

        self._ctx = None

        self._conn_fut = asyncio.get_running_loop().create_future()
        self._disc_fut = None

        self._recv_buffer = None
        self._data_sink = None

        self._landed_queue = deque()
        self._recv_obj_waiters = deque()

    @property
    def ctx(self):
        return self._ctx

    @ctx.setter
    def ctx(self, ctx):
        init_magic = ctx.get("__hbi_init__", None)
        if init_magic is not None:
            init_magic(self.po, self)
        self._ctx = ctx

    async def connected(self):
        await self._conn_fut

    async def co_recv_obj(self):
        # TODO
        pass

    async def co_recv_data(self, bufs):
        # TODO
        pass

    async def co_send_code(self, code):
        po = self.po
        # TODO add task to passive hosting conversation queue

    async def co_send_data(self, bufs):
        po = self.po
        # TODO add task to passive hosting conversation queue

    async def disconnect(self, err_reason=None, try_send_peer_err=True):
        _wire = self._wire
        if _wire is None:
            raise asyncio.InvalidStateError(
                f"HBI {self.net_ident} hosting endpoint not wired yet!"
            )

        disc_fut = self._disc_fut
        if disc_fut is not None:
            if err_reason is not None:
                logger.error(
                    rf"""
HBI {self.net_ident} repeated disconnection due to error:
{err_reason}
""",
                    stack_info=True,
                )
            await disc_fut
            return

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

        if self.po is not None:
            # close posting endpoint (i.e. write eof) before closing the socket
            await self.po.disconnect(err_reason, try_send_peer_err)

        elif err_reason is not None and try_send_peer_err:
            logger.warning(
                f"HBI {self.net_ident} not sending peer error {err_reason!s} as no posting endpoint.",
                exc_info=True,
            )

        try:
            _wire.transport.close()
        except OSError:
            # may already be invalid
            pass
        # connection_lost will be called by asyncio loop after out-going packets flushed

        await disc_fut

    async def disconnected(self):
        disc_fut = self._disc_fut
        if disc_fut is None:
            raise asyncio.InvalidStateError(f"HBI {self.net_ident} not disconnecting")
        await disc_fut

    # should be called by wire protocol
    def _connected(self, net_ident):
        self.net_ident = net_ident

        conn_fut = self._conn_fut
        assert conn_fut is not None, "?!"
        if conn_fut.done():
            assert fut.exception() is None and fut.result() is None, "?!"
        else:
            conn_fut.set_result(None)

        self._send_mutex.startup()

    # should be called by wire protocol
    def _data_received(self, chunk):
        # push to buffer
        if chunk:
            self._recv_buffer.append(BytesBuffer(chunk))

        while True:  # consume as much data as possible from wire

            # feed as much buffered data as possible to data sink if present
            while self._data_sink:
                # make sure data keep flowing in regarding lwm
                if self._recv_water_pos() <= self.low_water_mark_recv:
                    self._resume_recv()

                if self._recv_buffer is None:
                    # unexpected disconnect
                    self._data_sink(None)
                    return

                chunk = self._recv_buffer.popleft()
                if not chunk:
                    # no more buffered data, wire is empty, return
                    return
                self._data_sink(chunk)

            # try consume all landed but pending consumed objects first
            while self._landed_queue:
                while self._recv_obj_waiters:
                    obj_waiter = self._recv_obj_waiters.popleft()
                    if obj_waiter.done():
                        assert (
                            obj_waiter.cancelled() or obj_waiter.exception is not None
                        ), "got result already ?!"
                        # ignore a waiter whether it is cancelled or met other exceptions
                        continue  # find next waiter to receive the landed value
                    landed = self._landed_queue.popleft()
                    if len(landed) == 3:
                        co_task = landed[1]
                        chain_future(co_task, obj_waiter)
                    else:
                        assert len(landed) == 2, "?!"
                        if landed[0] is None:
                            obj_waiter.set_result(landed[1])
                        else:
                            obj_waiter.set_exception(landed[0])
                    if not self._landed_queue:
                        break
                if not self._recv_obj_waiters:
                    break

            while True:
                if self._disconnecting:
                    return

                # ctrl incoming flow regarding hwm/lwm
                buffered_amount = self._recv_water_pos()
                if buffered_amount > self.high_water_mark_recv:
                    self._pause_recv()
                elif buffered_amount <= self.low_water_mark_recv:
                    self._resume_recv()

                # land any packet available from wire
                landed = self._land_one()
                if landed is None:
                    # no more packet to land
                    return
                if len(landed) == 1:
                    # this landed packet is not interesting to application layer
                    continue
                if len(landed) not in (2, 3):
                    raise RuntimeError(
                        f"land result is {type(landed).__name__} of {len(landed)} ?!"
                    )

                if self._co_remote_ack is None and self._co_local_ack is None:
                    # ignore landed result if not in active or passive corun conversation
                    pass
                else:
                    # queue landed packet
                    self._landed_queue.append(landed)

                    if len(self._landed_queue) >= self.app_queue_size:
                        # stop reading wire and pause network recv if landing queue becomes big
                        self._pause_recv()
                        return

                # try give queue head to a waiter
                if not self._recv_obj_waiters or not self._landed_queue:
                    # not possible
                    continue
                landed = self._landed_queue.popleft()
                given_to_a_waiter = False
                while self._recv_obj_waiters:
                    obj_waiter = self._recv_obj_waiters.popleft()
                    if obj_waiter.done():
                        assert (
                            obj_waiter.cancelled() or obj_waiter.exception is not None
                        ), "got result already ?!"
                        # ignore a waiter whether it is cancelled or met other exceptions
                        continue  # find next waiter to receive the landed value
                    if len(landed) == 3:
                        co_task = landed[1]
                        chain_future(co_task, obj_waiter)
                    else:
                        assert len(landed) == 2, "?!"
                        if landed[0] is None:
                            obj_waiter.set_result(landed[1])
                        else:
                            obj_waiter.set_exception(landed[0])
                    given_to_a_waiter = True
                    break  # landed value given to a waiter, done for it
                if not given_to_a_waiter:
                    # put back to queue head if not consumed by a waiter
                    self._landed_queue.appendleft(landed)

    # should be called by wire protocol
    def _peer_eof(self):
        # returning True here to prevent the socket from being closed automatically
        peer_done_cb = self.context.get("hbi_peer_done", None)
        if peer_done_cb is not None:
            maybe_coro = peer_done_cb()
            if inspect.iscoroutine(maybe_coro):
                # the callback is a coroutine, assuming the socket should not be closed on peer eof
                asyncio.get_running_loop().create_task(maybe_coro)
                return True
            else:
                # the callback is not coroutine, its return value should reflect its intent
                return maybe_coro
        return False  # let the socket be closed automatically

    # should be called by wire protocol
    def _disconnected(self, exc=None):
        if exc is not None:
            logger.warning(
                f"HBI {self.net_ident} connection unwired due to error: {exc}"
            )

        # abort pending tasks
        self._recv_buffer = None  # release this resource
        if self._recv_obj_waiters:
            if exc is None:
                exc = RuntimeError(f"HBI {self.net_ident} disconnected")
            waiters = self._recv_obj_waiters
            self._recv_obj_waiters = None  # release this resource
            for waiter in waiters:
                if waiter.done():
                    # may have been cancelled etc.
                    continue
                waiter.set_exception(exc)

        disc_fut = self._disc_fut
        if disc_fut is None:
            disc_fut = self._disc_fut = asyncio.get_running_loop().create_future()
        elif not disc_fut.done():
            disc_fut.set_result(exc)

        disconn_cb = self.context.get("hbi_disconnected", None)
        if disconn_cb is not None:
            maybe_coro = disconn_cb(exc)
            if inspect.iscoroutine(maybe_coro):
                asyncio.create_task(maybe_coro)
