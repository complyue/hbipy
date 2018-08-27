"""
Service Consumer serving endpoint

"""

from typing import *

import hbi
from hbi import me
from hbi.log import get_logger
from hbi.pool import pe
from hbi.pool.mgmt import *

assert '__hbi_pool_consumer_peer__' == __name__, 'this only meant to run as peer of service pool consumer!'

logger = get_logger(__package__)

hbi_peer: hbi.HBIC = None  # will be updated by HBI after module initialization

consumer: ServiceConsumer = None


def hbi_connected():
    global consumer

    logger.debug(f'HBI pool consumer peer {hbi_peer} connected.')

    assert consumer is None, 'dirty mcp module ?!'
    consumer = ServiceConsumer(hbi_peer)


def hbi_boot():
    hbi_peer.disconnect('You normally connect to HBI pool via hbi.pool.ServiceMaster()')


async def assign_proc(session: str = None, sticky: Optional[bool] = None):
    consumer.session = session
    if sticky is not None:
        consumer.sticky = bool(sticky)
    proc_port = await pe.master.assign_proc(consumer)
    return {
        'host': me.host, 'port': proc_port,
    }


def release_proc():
    pe.master.release_proc(consumer)


def hbi_disconnected(exc=None):
    global consumer

    if exc is not None:
        logger.warning(f'HBI consumer disconnected due to error: {exc}')
    else:
        logger.debug(f'HBI consumer disconnected.')

    if consumer is not None:
        pe.master.release_proc(consumer)
        consumer = None
