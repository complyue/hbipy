import asyncio
import json
import os
import os.path
import runpy
import signal
import subprocess
import sys
import time
import traceback
from math import nan
from typing import *

from hbi.log import get_logger
from hbi.sockconn import HBIC

__all__ = [
    'PoolMaster', 'ProcWorker', 'ServiceConsumer',
]

logger = get_logger(__name__)

PROC_ALIVE_CHECK = 10  # todo expose as config option


class ServiceConsumer:
    """
    micro service pool consumer

    """
    __slots__ = 'hbi_peer', 'session', 'sticky', 'assigned_worker',

    def __init__(self, hbi_peer: HBIC, sticky: bool = True):
        self.hbi_peer: HBIC = hbi_peer
        self.session: str = None
        self.sticky: bool = bool(sticky)
        self.assigned_worker: ProcWorker = None


class PoolMaster:
    """
    micro service pool master

    """

    __slots__ = (
        'pool_size', 'hot_back', 'ack_timeout',
        'team_addr', '_pool_hbis_task', '_all_workers', '_pending_workers', '_idle_workers',
        '_workers_by_pid',
        '_workers_by_session',
    )

    def __init__(
            self,
            pool_size: int = max(1, os.cpu_count() // 2),
            hot_back: int = 2,
            *,
            ack_timeout: float = 10.0,
    ):
        self.pool_size = int(pool_size)
        assert self.pool_size >= 1
        self.hot_back = int(hot_back)
        assert self.hot_back >= 0
        self.ack_timeout = float(ack_timeout)
        assert self.ack_timeout > 0.0

        self.team_addr = None
        self._pool_hbis_task = None
        self._all_workers = set()
        self._pending_workers = set()
        self._idle_workers = asyncio.Queue()
        self._workers_by_pid = {}
        self._workers_by_session = {}

    def start_pool(self, loop=None):
        assert self._pool_hbis_task is None, 'start an already started pool ?!'

        if loop is None:
            loop = asyncio.get_event_loop()

        self.team_addr = {
            # let os assign arbitrary port on loopback interface
            'host': '127.0.0.1', 'port': None,
        }
        self._pool_hbis_task = loop.create_task(HBIC.create_server(
            lambda: runpy.run_module(f'{__package__}._pmgr.__main__',
                                     run_name='__hbi_pool_master__'),
            addr=self.team_addr, loop=loop,
        ))
        loop.create_task(self._pool_cleanup())

        def got_team_port(fut):
            server = fut.result()
            self.team_addr['port'] = server.sockets[0].getsockname()[1]
            logger.info(f'HBI Service Pool team addr: {self.team_addr}')

            self._all_workers.clear()
            for _ in range(min(self.hot_back, self.pool_size)):
                self._all_workers.add(ProcWorker(self))

        self._pool_hbis_task.add_done_callback(got_team_port)

    async def _pool_cleanup(self):
        if self._pool_hbis_task is not None:
            hbis = await self._pool_hbis_task
            await hbis.wait_closed()
            self._pool_hbis_task = None
        if self.team_addr is not None:
            try:
                os.removedirs(os.path.dirname(self.team_addr))
            except OSError:
                logger.warning(f'Failed cleaning up pool temp dir: {self.team_addr}', exc_info=True)
            self.team_addr = None

        self._all_workers.clear()
        self._pending_workers.clear()
        self._idle_workers = asyncio.LifoQueue()
        self._workers_by_pid.clear()
        self._workers_by_session.clear()

    def status(self):
        if self._pool_hbis_task is None:
            return 'NotStarted'
        if self._pool_hbis_task.done():
            if self._pool_hbis_task.cancelled():
                return 'Cancelled'
            if self._pool_hbis_task.exception() is not None:
                return 'InError'
            return 'Started'
        return 'Starting'

    def stop_pool(self):
        if self._pool_hbis_task is None:  # stopped or never started
            return
        # close the hbi server to stop accepting new connections,
        # cleanup will be triggered after close of server socket
        # todo add forceful option to allow termination of active connections
        self._pool_hbis_task.add_done_callback(lambda fut: fut.result().close())

    def register_proc(self, pid, hbi_peer) -> Optional['ProcWorker']:
        worker = self._workers_by_pid.get(pid, None)
        if worker is None:
            hbi_peer.disconnect(f'Your pid {pid} is unknown!')
            return None
        if worker.po.pid != pid:
            hbi_peer.disconnect(f'Your pid mismatches the expected {worker.po.pid!r}')
            return None
        logger.info(f'{worker!r} reported in.')
        worker.hbi_peer = hbi_peer
        worker.last_act_time = time.time()
        return worker

    async def worker_serving(self, worker: 'ProcWorker'):
        self._pending_workers.discard(worker)
        self._idle_workers.put_nowait(worker)

    async def assign_proc(self, consumer: ServiceConsumer):
        # TODO implement load based provisional assignment

        # maintain enough hot backing proc workers
        idle_quota = self.hot_back - (self._idle_workers.qsize() + len(self._pending_workers))
        if idle_quota > 0:
            logger.debug(f'Starting {idle_quota!r} hot backing proc workers given current workers:'
                         f' ({self._idle_workers.qsize()}/{len(self._all_workers)}/{self.pool_size})')
        while idle_quota > 0 and len(self._all_workers) < self.pool_size:
            self._all_workers.add(ProcWorker(self))
            idle_quota -= 1

        # check repeated assignment requests
        worker = consumer.assigned_worker
        if worker is not None and worker.check_alive():
            # this consumer have had a worker assigned, and still alive
            if worker.last_session != consumer.session:
                # consumer changed session
                if consumer.session is not None and consumer.sticky:
                    raise RuntimeError(
                        f'{consumer} requested sticky session but changed session itself ?!')
                # prepare for new session if changed
                if worker.last_session is not None:
                    if self._workers_by_session.get(worker.last_session, None) is worker:
                        del self._workers_by_session[worker.last_session]
                    worker.last_session = None
                if consumer.session is not None:
                    self._workers_by_session[consumer.session] = worker
                await worker.prepare_session(consumer.session)
                assert worker.last_session == consumer.session
                return worker.proc_port

        # if the consumer requested sticky session, assign the session worker regardless of idle status,
        # concurrency/parallelism is offloaded to the proc worker process
        if consumer.sticky and consumer.session is not None:
            sticky_session = consumer.session
            worker: 'ProcWorker' = self._workers_by_session.get(sticky_session, None)
            if worker is not None and worker.check_alive():
                if worker.last_session == sticky_session:
                    # a sticky session worker is alive, assign it
                    consumer.assigned_worker = worker
                    return worker.proc_port
                if worker.last_session is not None:
                    raise RuntimeError(
                        f'Worker by session dict corrupted ?! [{sticky_session}] => [{worker.last_session}]'
                    )
                # this worker is still preparing the sticky session, assign it after preparation finished,
                # this ensures concurrent assignment requests for a same sticky session won't trigger multiple
                # proc workers
                prepared_session = await worker.prepared_session
                if prepared_session == sticky_session:
                    return worker.proc_port

        # no sticky session requested, or no proc worker for the requested session yet

        # find an idle worker for this consumer, prefer adhere session
        searched_workers = set()
        while True:
            worker: ProcWorker = await self._idle_workers.get()
            if not worker.check_alive():
                # this worker already dead, should have been restarting,
                # try next idle worker in queue
                continue

            # got an idle worker
            if consumer.session is None:
                # no session designated
                if worker.last_session is None:
                    # found a worker lastly worked for no session, this considered an ideal match
                    break
            else:
                # with a designated session from consumer
                if worker.last_session == consumer.session:
                    # found the ideal worker lastly worked for the requested session
                    break

            if worker in searched_workers:
                # reached a known idle worker, meaning we've searched all idle workers without ideal session match,
                # use this worker anyway
                break

            # record this idle worker as known by first pass search, and back-queue this idle worker
            searched_workers.add(worker)
            self._idle_workers.put_nowait(worker)
            # as we are still in first pass of search, we'd pursue an ideal session match

        if worker.last_session != consumer.session:
            if worker.last_session is not None:
                if self._workers_by_session.get(worker.last_session, None) is worker:
                    del self._workers_by_session[worker.last_session]
                worker.last_session = None
            if consumer.session is not None:
                self._workers_by_session[consumer.session] = worker
            await worker.prepare_session(consumer.session)
            assert worker.last_session == consumer.session
            consumer.assigned_worker = worker

        return worker.proc_port

    def release_proc(self, consumer: ServiceConsumer):
        worker = consumer.assigned_worker
        if worker is None:
            return
        consumer.assigned_worker = None
        if worker.check_alive():
            self._idle_workers.put_nowait(worker)


class ProcWorker:
    """
    micro service pool proc worker, start, kill and restart a subprocess as necessary.

    """
    __slots__ = (
        'master',
        'po',
        'hbi_peer',
        'proc_port', 'load',
        'last_act_time', 'last_session', 'prepared_session',
    )

    def __init__(self, master: PoolMaster):
        self.master = master
        self.po = None
        self.hbi_peer: HBIC = None
        self.proc_port = None
        self.load = nan
        self.last_act_time = 0.0
        self.last_session = None
        self.prepared_session = asyncio.get_event_loop().create_future()
        self.prepared_session.set_result(self.last_session)

        self.start_worker_process()

    def __repr__(self):
        if self.po is None:
            return 'ProcWorker[unborn]'
        return f'ProcWorker[{self.po.pid}]'

    def start_worker_process(self):
        session = self.last_session
        if session is not None:
            session_worker = self.master._workers_by_session.get(session, None)
            if session_worker is self:
                del self.master._workers_by_session[session]
            self.last_session = None

        # pydev debuggers have difficulties figuring out subprocesses to be started with `python -m <modu>` cmdl,
        # have to use the helper script
        import hbi
        hbi_dir = os.path.dirname(hbi.__file__)
        hbipy_dir = os.path.dirname(hbi_dir)
        # a proc subprocess should live together with its master process, and killed altogether
        self.po = subprocess.Popen([
            sys.executable, f'{hbipy_dir}/run-module.py',
            f'{__package__}._pmgr.worker', json.dumps(self.master.team_addr)
        ], start_new_session=False, )
        self.hbi_peer = None
        self.proc_port = None
        self.load = nan
        self.last_act_time = 0.0
        self.last_session = None
        self.master._workers_by_pid[self.po.pid] = self
        self.master._pending_workers.add(self)

    def restart_worker_process(self, err_reason: Optional[str]):
        hbi_peer = self.hbi_peer
        if hbi_peer is not None:
            self.hbi_peer = None
            self.proc_port = None
            self.load = nan
            self.last_act_time = 0.0
            self.last_session = None
            try:
                # the disconnection along would make the worker process terminate
                hbi_peer.disconnect(err_reason)
            except Exception:
                logger.warning(f'Failed disconnecting proc worker for reason {err_reason!s}',
                               exc_info=True)

        if self.po is None:
            logger.warning('Restarting proc worker without ever started ?!')
        else:
            pid = self.po.pid
            logger.debug(f'Restarting proc worker pid={pid} due to reason: {err_reason!s}')
            try:
                # make an attempt to interrupt it for quicker shutdown, in case it is hogging CPU or other resources
                os.kill(pid, signal.SIGINT)
                self.po = None
            except OSError:
                # can already been dead, and other failures sending the signal
                pass
            self.master._workers_by_pid[pid] = None

        self.start_worker_process()

    def check_alive(self):
        assert self.po is not None

        if self.po.poll() is not None:
            # this subprocess got a returncode, but it may because a debugger is attached,
            # check via os signal
            try:
                os.kill(self.po.pid, 0)
                # proc process pid still exists, most likely a debugger is attached,
                # we'd live with it unless unresponsive
            except OSError:
                # this proc worker process is really dead
                self.start_worker_process()
                return False

        if self.hbi_peer is not None and self.hbi_peer.connected:
            if time.time() - self.last_act_time < PROC_ALIVE_CHECK:
                # throttle ctrl
                return True
            try:
                self.hbi_peer.fire('ping()')
                return True
            except Exception:
                logger.error(f'Connection to proc with pid {self.po.pid} seems dead.',
                             exc_info=True)

        logger.warning(f'Killing zombie proc with pid {self.po.pid} ...')
        try:
            self.po.kill()
        except OSError:
            traceback.print_exc()
        self.start_worker_process()
        return False

    async def report_serving(self, port: int):
        self.proc_port = port
        self.load = 0.0
        await self.master.worker_serving(self)

    async def prepare_session(self, session: str = None):
        prepared_session = self.prepared_session = asyncio.get_event_loop().create_future()
        try:
            async with self.hbi_peer.co() as hbi_peer:
                confirmed_session = await hbi_peer.co_get(rf'''
prepare_session({session!r})
''')
                assert confirmed_session == session
                self.last_session = confirmed_session
            prepared_session.set_result(confirmed_session)
        except Exception as exc:
            prepared_session.set_exception(exc)
            raise
