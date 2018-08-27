"""
HBI pool proc worker subprocess peer within master process

"""

import hbi
from hbi import me
from hbi.log import get_logger
from hbi.pool import pe

from ..mgmt import ProcWorker

logger = get_logger(__package__)

assert '__hbi_pool_master__' == __name__, \
    'this only meant to run as the peer of an M3 project worker subprocess!'

hbi_peer: hbi.HBIC = None  # will be updated by HBI after module initialization

worker: ProcWorker = None

# will be bound worker.report_serving() method
worker_serving = None


def worker_online(pid: int):
    global worker, worker_serving
    assert worker is None, 'worker subprocess repeating online ?!'
    worker = pe.master.register_proc(pid, hbi_peer)
    assert worker is not None, 'spawned worker not tracked ?!'

    worker_serving = worker.report_serving

    hbi_peer.send_notification(rf'''
serv_hbi_module(
    { {k:vars(me)[k] for k in me.__share__} !r},
    { {k:vars(pe)[k] for k in pe.__share__} !r},
)
''')


def ping():
    hbi_peer.fire('pong()')


def pong():
    pass


def hbi_disconnected(exc=None):
    if exc is not None:
        logger.warning(
            f'{worker} disconnected due to error: {exc}'
        )
    else:
        logger.debug(f'{worker} disconnected.')

    worker.check_alive()
