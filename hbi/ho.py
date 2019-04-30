import asyncio
import inspect
from typing import *

from ._details import *
from .context import run_in_context
from .log import *

__all__ = ["HostingEnd"]

logger = get_logger(__name__)


class HostingEnd:
    """
    HBI hosting endpoint

    """

    def __init__(self, po, app_queue_size: int = 100):
        self.po = po

        self._wire = None
        self.net_ident = "<unwired>"

        self._ctx = None

        self._conn_fut = asyncio.get_running_loop().create_future()
        self._disc_fut = None

        # hosting green-thread
        self._hoth = None
        # the event indicating a hosting task is running
        self._hotr = asyncio.Event()
        # the hosting task that is currently running
        self._hott = None

        # queue for received objects
        self._horq = asyncio.Queue(app_queue_size)

        # for the active posting conversation
        # hold a reference to po._coq head during co_ack_begin/co_ack_end from remote peer
        self._po_co = None

        # for the passive hosting conversation
        # hold the coid value during co_begin/co_end from remote peer
        self._ho_coid = None

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
        obj = await self._recv_obj()
        if self._ho_coid is None:
            raise asyncio.InvalidStateError(f"Object received out of conversation!")
        return obj

    async def _recv_obj(self):
        self._wire._check_resume()
        return await self._horq.get()

    async def co_recv_data(self, bufs):
        if self._ho_coid is None:
            raise asyncio.InvalidStateError("No hosting conversation!")

        await self._recv_data(bufs)

    async def co_send_code(self, code):
        if self._ho_coid is None:
            raise asyncio.InvalidStateError("No hosting conversation!")

        po = self.po
        await po._send_code(code, b"co_recv")

    async def co_send_data(self, bufs):
        if self._ho_coid is None:
            raise asyncio.InvalidStateError("No hosting conversation!")

        po = self.po
        await p._send_data(bufs)

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

        assert self._hoth is None, "hosting task created already ?!"
        self._hoth = asyncio.create_task(self._ho_thread())

    async def _ho_thread(self):
        """
        Use a green-thread to guarantee serial execution of packets transfered over the wire.

        Though the code from a packet can spawn new green-threads by calling `asyncio.create_task()`
        during landing.

        """

        hotr = self._hotr
        wire = self._wire
        while True:
            try:
                await hotr.wait()
            except asyncio.CancelledError:
                assert (
                    self._disc_fut is not None
                ), "hosting thread cancelled while not disconnecting ?!"
                break

            while wire._land_one():  # consume as much from wire buffer

                coro = self._hott
                if coro is None:  # no coroutine to run from last packet landing
                    continue  # proceed to land next packet
                self._hott = None

                try:

                    await coro  # run the coroutine by awaiting it

                except asyncio.CancelledError:
                    if self._disc_fut is not None:
                        # disconnecting, stop hosting thread
                        break

                    logger.warning(
                        f"HBI {self.net_ident!s} a hosted task cancelled: {coro}",
                        exc_info=True,
                    )
                except Exception as exc:
                    logger.error(
                        f"HBI {self.net_ident!s} a hosted task failed: {coro}",
                        exc_info=True,
                    )
                    self.disconnect(exc)
                    # self._hoth task (which is running this function) should have been cancelled,
                    # as part of the disconnection, next await on hotr should get CancelledError,
                    # then return out.

                    # todo: opt to return immediately here ?
                    # return

            # no more packet to land atm, wait for new data arrival
            hotr.clear()

    async def _ack_co_begin(self, coid: str):
        if self._ho_coid is not None:
            raise asyncio.InvalidStateError("Unclean co_begin!")
        else:
            await self.po._send_text(coid, b"co_ack_begin")
            self._ho_coid = coid

    async def _ack_co_end(self, coid: str):
        if self._ho_coid != coid:
            raise asyncio.InvalidStateError("Unclean co_end!")
        else:
            await self.po._send_text(coid, b"co_ack_end")
            self._ho_coid = None

    async def _co_begin_acked(self, coid: str):
        if self._po_co is not None:
            raise asyncio.InvalidStateError("Unclean co_ack_begin!")

        self._po_co = self.po._co_begin_acked(coid)

    async def _co_end_acked(self, coid: str):
        if self._po_co is None:
            raise asyncio.InvalidStateError("Unclean co_ack_end!")

        co = self.po._co_end_acked(coid)
        if co is not self._po_co:
            raise asyncio.InvalidStateError("Mismatched co_ack_end!")

        self._po_co = None

    async def _co_send_back(self, obj):
        if inspect.isawaitable(obj):
            obj = await obj
        await self._send_text(repr(obj), b"co_recv")

    async def _co_recv_landed(self, obj):
        if self._po_co is None and self._ho_coid is None:
            raise asyncio.InvalidStateError("No conversation to recv!")

        if self._horq.full():
            self._wire._check_pause()

        if inspect.isawaitable(obj):
            obj = await obj

        await self._horq.put(obj)

    def _land_packet(self, code, wire_dir) -> Optional[tuple]:
        assert (
            self._hott is None
        ), "landing new packet while last not finished running ?!"
        assert not self._hotr.is_set(), "hotr set on landing new packet ?!"

        if "co_begin" == wire_dir:

            self._hott = self._ack_co_begin(code)
            self._hotr.set()

        elif "co_recv" == wire_dir:
            # peer is sending a result object to be received by this end

            landed = self._land_code(code)
            self._hott = self._co_recv_landed(landed)
            self._hotr.set()

        elif "co_send" == wire_dir:
            # peer is requesting this end to send landed result back

            landed = self._land_code(code)
            self._hott = self._co_send_back(landed)
            self._hotr.set()

        elif "co_end" == wire_dir:

            self._hott = self._ack_co_end(code)
            self._hotr.set()

        elif "co_ack_begin" == wire_dir:

            self._hott = self._co_begin_acked(code)
            self._hotr.set()

        elif "co_ack_end" == wire_dir:

            self._hott = self._co_end_acked(code)
            self._hotr.set()

        elif "err" == wire_dir:
            # peer error

            self._handle_peer_error(code)

        else:

            self._handle_landing_error(
                RuntimeError(f"HBI unknown wire directive [{wire_dir}]")
            )

    def _land_code(self, code):
        # allow customization of code landing
        lander = self.context.get("__hbi_land__", None)
        if lander is not None:
            assert callable(lander), "non-callable __hbi_land__ defined ?!"
            try:

                landed = lander(code)
                if NotImplemented is not landed:
                    # custom lander can return `NotImplemented` to proceed standard landing
                    return landed

            except Exception as exc:
                logger.debug(
                    rf"""
HBI {self.net_info}, error custom landing code:
--CODE--
{code!s}
--====--
""",
                    exc_info=True,
                )
                self._handle_landing_error(exc)
                raise

        # standard landing
        defs = {}
        try:

            landed = run_in_context(code, self.context, defs)
            return landed

        except Exception as exc:
            logger.debug(
                rf"""
HBI {self.net_ident}, error landing code:
--CODE--
{code!s}
--DEFS--
{defs!r}
--====--
""",
                exc_info=True,
            )
            self._handle_landing_error(exc)
            raise

    async def _recv_data(self, bufs):
        self._wire._check_resume()

        fut = asyncio.get_running_loop().create_future()

        # use a generator function to pull all buffers from hierarchy
        def pull_from(boc):
            b = cast_to_tgt_buffer(
                boc
            )  # this static method can be overridden by subclass
            if b:
                yield b
            else:
                for boc1 in boc:
                    yield from pull_from(boc1)

        pos = 0
        buf = None

        buf_puller = pull_from(bufs)

        def data_sink(chunk):
            nonlocal pos
            nonlocal buf

            if chunk is None:
                if not fut.done():
                    fut.set_exception(RuntimeError("HBI disconnected"))
                self._wire._end_offload(None, data_sink)

            try:
                while True:
                    if buf is not None:
                        assert pos < len(buf)
                        # current buffer not filled yet
                        if not chunk or len(chunk) <= 0:
                            # data expected by buf, and none passed in to this call,
                            # return and expect next call into here
                            self._resume_recv()
                            return
                        available = len(chunk)
                        needs = len(buf) - pos
                        if available < needs:
                            # add to current buf
                            new_pos = pos + available
                            buf[pos:new_pos] = chunk.data()
                            pos = new_pos
                            # all data in this chunk has been exhausted while current buffer not filled yet
                            # return now and expect succeeding data chunks to come later
                            self._resume_recv()
                            return
                        # got enough or more data in this chunk to filling current buf
                        buf[pos:] = chunk.data(0, needs)
                        # slice chunk to remaining data
                        chunk.consume(needs)
                        # clear current buffer pointer
                        buf = None
                        pos = 0
                        # continue to process rest data in chunk, even chunk is empty now, still need to proceed for
                        # finish condition check

                    # pull next buf to fill
                    try:
                        buf = next(buf_puller)
                        pos = 0
                        if len(buf) == 0:  # special case for some empty data
                            buf = None
                    except StopIteration as ret:
                        # all buffers in hierarchy filled, finish receiving
                        self._wire._end_offload(chunk, data_sink)
                        # resolve the future
                        if not fut.done():
                            fut.set_result(bufs)
                        # and done
                        return
                    except Exception as exc:
                        raise RuntimeError(
                            "HBI buffer source raised exception"
                        ) from exc
            except Exception as exc:
                self._handle_wire_error(exc)
                if not fut.done():
                    fut.set_exception(exc)

        self._wire._begin_offload(data_sink)

        return await fut

    # should be called by wire protocol
    def _peer_eof(self):
        if self._po_co is not None or self._ho_coid is not None:
            # make sure coroutines receiving from current conversation get the exception, or they'll leak
            exc = asyncio.InvalidStateError(
                "Premature peer EOF before conversation ended."
            )
            if self._po_co is not None:
                for fut in self._po_co._co_receivers:
                    if not fut.done():
                        fut.set_exception(exc)
            if self._ho_coid is not None:
                for fut in self._ho_receivers:
                    if not fut.done():
                        fut.set_exception(exc)
            raise exc

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

    def _handle_landing_error(self, exc):
        self.disconnect(exc)

    def _handle_peer_error(self, err_reaon):
        self.disconnect(f"HBI peer error: {err_reaon}", False)

    # should be called by wire protocol
    def _disconnected(self, exc=None):
        if exc is not None:
            logger.warning(
                f"HBI {self.net_ident} connection unwired due to error: {exc}"
            )

        if self._po_co is not None:
            # abort pending receivers from posting conversation
            if exc is None:
                exc = RuntimeError(f"HBI {self.net_ident} disconnected")
            for fut in self._po_co._co_receivers:
                if fut.done():  # may have been cancelled etc.
                    continue
                fut.set_exception(exc)

        if self._ho_coid is not None:
            # abort pending receivers from hosting conversation
            if exc is None:
                exc = RuntimeError(f"HBI {self.net_ident} disconnected")
            for fut in self._ho_receivers:
                if fut.done():  # may have been cancelled etc.
                    continue
                fut.set_exception(exc)

        disc_fut = self._disc_fut
        if disc_fut is None:
            disc_fut = self._disc_fut = asyncio.get_running_loop().create_future()
        if not disc_fut.done():
            disc_fut.set_result(exc)

        disconn_cb = self.context.get("hbi_disconnected", None)
        if disconn_cb is not None:
            maybe_coro = disconn_cb(exc)
            if inspect.iscoroutine(maybe_coro):
                asyncio.get_running_loop().create_task(maybe_coro)

        ho_thread = self._hoth
        if ho_thread is not None:
            ho_thread.cancel()
