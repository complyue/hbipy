"""
Micro service Consumer Peer

"""

import hbi

from hbi import me

from hbi.log import get_logger

from hbi.pool.mgmt import *

assert '__hbi_pool_consumer_peer__' == __name__, 'this only meant to run as peer of service pool consumer!'

logger = get_logger(__package__)

hbi_peer: hbi.HBIC = None  # will be updated by HBI after module initialization

if hbi_peer is not None:  # will never exec, for IDE hinting only
    master: MicroMaster = MicroMaster()

consumer: MicroConsumer = None


def hbi_connected():
    global consumer

    assert consumer is None, 'dirty mcp module ?!'
    consumer = MicroConsumer(hbi_peer)


async def assign_proc(session: str = None):
    consumer.session = session
    proc_port = await master.assign_proc(consumer)
    await hbi_peer.co_send_code(repr({
        'host': me.host, 'port': proc_port,
    }))


def release_proc():
    master.release_proc(consumer)


def hbi_disconnected(exc=None):
    global consumer

    if exc is not None:
        logger.warning(f'HBI consumer disconnected due to error: {exc}')
    else:
        logger.debug(f'HBI consumer disconnected.')

    if consumer is not None:
        master.release_proc(consumer)
        consumer = None
