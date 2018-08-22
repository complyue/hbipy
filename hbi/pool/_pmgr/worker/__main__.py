"""
HBI pool proc worker subprocess booting module

"""
import asyncio
import os
import runpy
import sys

import psutil
from async_timeout import timeout

import hbi
from hbi import me
from hbi.aio import *
from hbi.log import get_logger
from hbi.pool import pe

logger = get_logger(__package__)

this_process = psutil.Process(os.getpid())

# modu run from python command line as HBI pool proc worker subprocess
assert '__main__' == __name__
assert len(sys.argv) == 2, 'HBI pool proc worker subprocess should be started with <team_addr> !'
pe.team_addr, = sys.argv[1:]


def hbi_connected():
    hbi_peer.fire_corun(rf'''
worker_online({os.getpid()!r})
''')


hbi_proc_session_changing_to = None


async def serv_hbi_module(me_dict: dict, pe_dict: dict):
    global hbi_proc_session_changing_to

    vars(me).update(me_dict)
    vars(pe).update(pe_dict)

    logger.info(f'Starting HBI proc with module [{me.modu_name}] on {me.host}')
    me.server = await hbi.HBIC.create_server(
        lambda: runpy.run_module(me.modu_name, init_globals=me.init_globals, run_name='__hbi_accepting__'),
        addr={
            'host': me.host, 'port': None,  # let os assign arbitrary port
        }, net_opts=me.net_opts, loop=me.loop
    )
    me.port = me.server.sockets[0].getsockname()[1]
    me.addr = {'host': me.host, 'port': me.port}
    logger.info(f'HBI proc serving [{me.modu_name}] on {me.addr}')

    modu_dict = runpy.run_module(me.modu_name, run_name='__hbi_serving__')
    hbi_proc_session_changing_to = modu_dict.get('hbi_proc_session_changing_to', None)
    if hbi_proc_session_changing_to is not None:
        assert callable(hbi_proc_session_changing_to)
        # todo validate that it accepts a single str arg as new session id

    await hbi_peer.send_corun(rf'''
worker_serving({me.port!r})
''')


async def prepare_session(session: str = None):
    if session != pe.session:
        if hbi_proc_session_changing_to is not None:
            # notify session change to HBI module if it defines a hook function
            hbi_proc_session_changing_to(session)
        pe.session = session
    await hbi_peer.co_send_code(repr(pe.session))


async def retire(force: bool = False):
    pe.retiring = True

    if pe.service_wip is not None and not pe.service_wip.done():
        # with pending service task(s)
        if force:
            logger.warning(f'forcefully retiring proc worker subprocess with pending service.')
        else:
            await pe.service_wip
    else:
        # no pending service task
        pass

    with timeout(10.0):
        await hbi_peer.wait_disconnected()

    sys.exit(5)


def ping():
    hbi_peer.fire('pong()')


def pong():
    pass


def hbi_disconnected(exc=None):
    if pe.retiring:
        if exc is not None:
            logger.warning(f'HBI proc worker subprocess {os.getpid()} retired with error: {exc!s}')
    else:
        logger.fatal(
            f'HBI proc worker subprocess {os.getpid()} unexpectedly disconnected from team, error: {exc!s}'
        )

    # try graceful worker process exit by stopping event loop
    me.loop.stop()

    # 10 seconds for graceful exit of the worker subprocess, or force exit with code 5
    me.loop.call_later(10, sys.exit, 5)


me.loop = asyncio.get_event_loop()
try:

    hbi_peer = hbi.HBIC(
        globals(), pe.team_addr, loop=me.loop
    )
    hbi_peer.run_until_connected()

    handle_signals()
    run_aio_servers()

finally:
    me.loop.close()
    sys.exit(0)
