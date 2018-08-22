"""
Micro Service Pool consumer interface

"""
import asyncio
import json
from concurrent import futures

from ..log import get_logger
from ..sockconn import HBIC

__all__ = [
    'MicroConnection',
]

logger = get_logger(__name__)


class MicroConnection:
    """
    """
    __slots__ = (
        'context', 'pool_hbic', 'proc_hbic', 'session',
        'co', 'ack_timeout', 'net_opts',
        'proc_conn_cache',
    )

    def __init__(
            self,
            context: dict, pool_addr: dict,
            *,
            session: str = None,
            net_opts: dict = None, ack_timeout: float = 10.0,
    ):
        self.context = context
        self.pool_hbic = HBIC(globals(), pool_addr, net_opts)
        self.proc_hbic = None
        self.session = session
        self.co = None
        self.ack_timeout = float(ack_timeout)
        self.net_opts = net_opts
        self.proc_conn_cache = {}

    @property
    def pool_addr(self):
        return self.pool_hbic.addr

    @property
    def proc_addr(self):
        if self.proc_hbic is None:
            return None
        return self.proc_hbic.addr

    async def __aenter__(self):
        await self.pool_hbic.wait_connected()
        async with self.pool_hbic.co() as pool_hbic:
            await pool_hbic.co_send_code(rf'''
assign_proc({self.session!r})
''')
            proc_addr = await pool_hbic.co_recv_obj()
            proc_cache_key = json.dumps(proc_addr)
            proc_hbic = self.proc_conn_cache.get(proc_cache_key, None)
            if proc_hbic is None:
                proc_hbic = HBIC(self.context, proc_addr, self.net_opts)
                self.proc_conn_cache[proc_cache_key] = proc_hbic
            await proc_hbic.wait_connected()
            self.proc_hbic = proc_hbic
        co = proc_hbic.co()
        hbic = await co.__aenter__()
        self.co = co

        logger.debug(f'Pool proc {hbic!r} bound for session {self.session!s} ...')
        return hbic

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        co = self.co
        if co is None:  # co not allocated yet
            return
        self.co = None

        if exc_type is not None:
            if issubclass(exc_type, (asyncio.CancelledError, futures.CancelledError)):
                err_reason = f'Pool master cancellation.'
            else:
                err_reason = f'Pool master error: {exc_type!s} {exc_val!s}'
            co.hbic.disconnect(err_reason)
            return
