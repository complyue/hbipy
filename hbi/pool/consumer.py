"""
Service Pool consumer interface

"""

import asyncio
import json

from ..log import get_logger
from ..sockconn import HBIC

__all__ = ["ServiceMaster"]

logger = get_logger(__name__)


class ServiceMaster:
    """
    """

    __slots__ = (
        "context",
        "master_hbic",
        "proc_hbic",
        "session",
        "ack_timeout",
        "net_opts",
        "proc_conn_cache",
    )

    def __init__(
        self,
        context: dict,
        master_addr: dict,
        *,
        session: str = None,
        net_opts: dict = None,
        ack_timeout: float = 10.0,
    ):
        self.context = context
        self.master_hbic = HBIC(globals(), master_addr, net_opts)
        self.proc_hbic = None
        self.session = session
        self.ack_timeout = float(ack_timeout)
        self.net_opts = net_opts
        self.proc_conn_cache = {}

        self.master_hbic.connect()

    def disconnect(self, err_reason: str = None, err_stack: str = None):
        # todo elaborate error handling here
        for k, c in self.proc_conn_cache.items():
            c.disconnect(err_reason, err_stack)
        self.master_hbic.disconnect(err_reason, err_stack)

    @property
    def master_addr(self):
        return self.master_hbic.addr

    @property
    def proc_addr(self):
        if self.proc_hbic is None:
            return None
        return self.proc_hbic.addr

    async def proc(self, session: str = None):
        proc_hbic = self.proc_hbic
        if session == self.session:
            # session not changed, can reuse connection to proc if still alive
            if proc_hbic is not None and proc_hbic.connected:
                # connection to proc still alive, no need to assign new proc
                return proc_hbic
        else:
            # new session or changed session, mandatory to ask service master for proc assignment,
            # though not unusual for it to assign same proc the consumer currently connected to
            self.session = session

        # request service master to assign a proc
        await self.master_hbic.wait_connected()
        async with self.master_hbic.co() as master_hbic:
            proc_addr = await master_hbic.co_get(
                rf"""
assign_proc({self.session!r})
"""
            )
            proc_cache_key = json.dumps(proc_addr)
            proc_hbic = self.proc_conn_cache.get(proc_cache_key, None)
            if proc_hbic is None:
                proc_hbic = HBIC(self.context, proc_addr, self.net_opts)
                self.proc_conn_cache[proc_cache_key] = proc_hbic
            else:
                proc_hbic.connect()

                if self.proc_hbic is not None and proc_hbic is not self.proc_hbic:
                    # assignment changed to a new proc
                    # TODO disconnect some cached proc connections according to some rules TBD
                    pass

            try:
                await proc_hbic.wait_connected()
            except Exception:
                # TODO complain to service master about unavailability of the proc,
                # retry according to some rules TBD
                raise

            self.proc_hbic = proc_hbic

        return self.proc_hbic

    def co(self, session: str = None):
        return ServiceCo(self, session)


class ServiceCo:
    """

    """

    __slots__ = "master", "session", "co"

    def __init__(self, master: ServiceMaster, session: str = None):
        self.master = master
        self.session = session
        self.co = None

    async def __aenter__(self):
        proc_hbic = await self.master.proc(self.session)
        co = proc_hbic.co()
        hbic = await co.__aenter__()
        self.co = co

        logger.debug(f"Pool proc {hbic!r} bound for session {self.session!s}")
        return hbic

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        co = self.co
        if co is None:  # co not allocated yet
            return
        self.co = None

        try:
            await co.__aexit__(exc_type, exc_val, exc_tb)
        except Exception:
            logger.fatal(f"HBI co context error.", exc_info=True)
            co.hbic.disconnect("HBI co context error.")
        else:
            if exc_type is not None:
                if issubclass(exc_type, (asyncio.CancelledError)):
                    err_reason = f"HBI service consumer cancellation."
                else:
                    err_reason = f"HBI service consumer error: {exc_type!s} {exc_val!s}"
                co.hbic.disconnect(err_reason)
