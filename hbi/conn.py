import asyncio
import socket
from typing import *

from ._details import *
from .ho import *
from .po import *

__all__ = ["HBIC", "HBIS"]

logger = logging.getLogger(__name__)


class HBIC:
    """
    HBI client over a socket

    Paradigm:

```python

import asyncio, hbi

def find_service(...):
    ...

def jobs_pending() -> bool:
    ...

async def fetch_next_job():
    # raises StopIteration
    ...

async def reschedule_job(job):
    ...

po2peer: hbi.PostingEnd = None
ho4peer: hbi.HostingEnd = None

def __hbi_init__(po, ho):
    global po2peer: hbi.PostingEnd
    global ho4peer: hbi.HostingEnd

    po2peer, ho4peer = po, ho

async def job_done(job_result):
    assert po2peer is not None and ho4peer is not None
    ...

async def work_out():
    service_addr = find_service(...)
    react_context = globals()
    hbic = hbi.HBIC(service_addr, react_context)

    while jobs_pending():
        job = None
        try:
            async with hbic as po:
                while job is not None or jobs_pending():
                    if job is None:
                        job = await fetch_next_job()
                    async with po.co() as co:
                        await co.send_code(rf'''
do_job({job.action!r}, {job.data_len!r})
''')
                        await co.send_data(job.data)

                        # !! try best to avoid such synchronous service calls !!
                        #job_result = await co.recv_obj()
                        # !! this holds back throughput REALLY !!

                        job = None
        except StopIteration:  # raised by fetch_next_job()
            break  # all jobs done
        except Exception:
            logger.error("Distributed job failure, retrying ...", exc_info=True)
            if job is not None:
                await reschedule_job(job)
                job = None
            await asyncio.sleep(RECONNECT_WAIT)

asyncio.run(work_out())

```

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
                host=addr.get("host", None),
                port=addr.get("port", None),  # let os assign a port
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

    Paradigm:

```python

import asyncio, hbi

if '__job_context__' == __name__:

    po2peer: hbi.PostingEnd = None
    ho4peer: hbi.HostingEnd = None

    def __hbi_init__(po, ho):
        global po2peer: hbi.PostingEnd
        global ho4peer: hbi.HostingEnd

        po2peer, ho4peer = po, ho

    async def do_job(action, data_len):
        global po2peer: hbi.PostingEnd
        global ho4peer: hbi.HostingEnd

        # can be plain binary blob
        job_data = bytearray(data_len)
        # or numpy array with shape&dtype infered from data_len
        #shape, dtype = data_len
        #job_data = np.empty(shape, dtype)

        await ho4peer.co_recv_data(job_data)

        # use action/job_data
        job_result = ...
        
        # !! try best to avoid such synchronous service calls !!
        #await ho4peer.co_send_code(repr(job_result))
        # !! this holds back throughput REALLY !!

        # it's best for throughput to use po2peer to post asynchronous notification back to service consumer
        await po2peer.notif(rf'''
job_done({job_result!r})
''')

else:

    serving_addr = {'host': '0.0.0.0', 'port': 1234}

    async def serve_jobs():
        hbis = hbi.HBIS(
            serving_addr,
            lambda po, ho: runpy.run_module(
                mod_name=__name__,  # reuse this module file
                run_name='__job_context__',  # distinguish by run_name
            ),  # create an isolated context for each consumer connection
        )
        await hbis.serve_until_closed()

    asyncio.run(serve_jobs())

```

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
                host=addr.get("host", None),
                port=addr.get("port", None),  # let os assign a port
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
