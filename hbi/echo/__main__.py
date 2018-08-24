"""
HBI Echo server and client

"""

import asyncio
import logging
import sys

import hbi
from hbi import me
from hbi.pool import MicroPool
from hbi.pool import pe

from .shell import *

logger = logging.getLogger(__package__)

if '__hbi_serving__' == __name__:
    # modu run per HBI server initialization

    if pe.master is not None:
        # run in service pool mode, this is in master process, me.host/me.port is consumer serving endpoint
        logger.info(f'Echo pool master listening {me.addr}')
    elif pe.team_addr is not None:
        # run in pool proc mode, me.host/me.port is direct serving endpoint
        logger.info(f'Echo server listening {me.addr}')
    else:
        # run in standalone server mode, me.host/me.port is direct serving endpoint
        logger.info(f'Echo pool proc listening {me.addr}')

elif '__hbi_accepting__' == __name__:
    # modu run per client HBI connection accepted at server side

    # hbi_peer is assigned after the module finished initialization, so it's okay to be used in functions defined here,
    # but during module initialization, it is None
    hbi_peer = None


    def hbi_boot():
        # running as echo server
        assert me.server is not None

        # if `hbi_boot` is triggered at server side, that means client modu doesn't provide an `hbi_boot`,
        # but this `hbi.echo` modu surely does. so here we know that peer is an unknown client modu,
        # just print warning message to it.
        hbi_peer.fire(r'''
import sys
print('This is echo server, merely serving `echo(*args, **kwargs)`, other code will obviously fail.', file=sys.stderr)
''')
        hbi_peer.disconnect()


    # net_info (so __str__) will be missing after disconnected, we need to save it if we want to report such info in
    # hbi_disconnected() callback.
    hbi_peer_name = 'HBI <never-connected>'


    def hbi_connected():
        global hbi_peer_name
        hbi_peer_name = str(hbi_peer)

        logger.info(f'HBI client {hbi_peer} connected.')


    def hbi_peer_done():
        logger.info(f'HBI client {hbi_peer} quited.')


    def hbi_disconnecting(err_reason=None):
        logger.warning(f'HBI client {hbi_peer} being disconnected ...')
        if err_reason is not None:
            logger.warning(f'  - Due to reason: {err_reason}')


    def hbi_disconnected(err_reason=None):
        logger.info(f'HBI client {hbi_peer_name} disconnected.')


    def echo(*args, **kwargs):
        # assuming peer is Python interpreter for now, language adaptive mechanisms could be added

        hbi_peer.fire(rf'''
print('\n--BEGIN-REMOTE-MSG--')
print(*({args!r}), **({kwargs!r}))
print('---END--REMOTE-MSG--', flush=True)
''')

elif __name__ in ('__hbi_connecting__', '__main__'):

    console: HBIConsole = None


    def hbi_disconnected(err_reason=None):
        if console is not None and console.running:
            # unexpected disconnection
            if err_reason is None:
                logger.info('HBI connection closed by peer.')
            else:
                logger.error(rf'''
HBI connection closed by peer due to error:
{err_reason}
''')
        sys.exit(1)


    def __hbi_land__(code, wire_dir):
        # this magic method if defined, hijacks code received over HBI wire for local execution (landing)

        print(rf'''
-== CODE TO LAND ==-
[#{wire_dir}]{code}
====================
''', flush=True)

        if console is not None and console.land_code:
            # perform normal landing, i.e. to execute it locally
            return NotImplemented


    if '__hbi_connecting__' == __name__:
        # run in standalone client mode
        # hbi_peer is assigned after the module finished initialization,
        # but during module initialization, it is None
        hbi_peer = None  # type: hbi.HBIC


        def hbi_boot():
            global console
            # running as echo client
            assert me.server is None
            console = HBIConsole(hbi_peer)
            console.run()

    elif '__main__' == __name__:
        # run in utility module mode, acting as micro service consumer

        def print_usage():
            print(r'''
HBI service pool interactive tact utility, usage:
  python -m hbi.echo [-6] [-p port] [-h host] [-s sock]
''', file=sys.stderr)


        async def main():
            global console

            if len(sys.argv) <= 1:
                return print_usage()

            arg_i = 0
            while arg_i + 1 < len(sys.argv):
                arg_i += 1

                if '--' == sys.argv[arg_i]:
                    me.argv = sys.argv[arg_i + 1:]
                    break

                if '-6' == sys.argv[arg_i]:
                    me.net_opts['family'] = socket.AF_INET6
                    continue

                if '-p' == sys.argv[arg_i]:
                    arg_i += 1
                    me.port = int(sys.argv[arg_i])
                    continue

                if '-h' == sys.argv[arg_i]:
                    arg_i += 1
                    me.host = sys.argv[arg_i]
                    continue

                if '-s' == sys.argv[arg_i]:
                    arg_i += 1
                    me.addr = sys.argv[arg_i]
                    continue

                if sys.argv[arg_i].startswith('-'):
                    return print_usage()

                return print_usage()

            if me.addr is None:
                if me.host is None:
                    return print_usage()
                me.addr = {'host': me.host, 'port': me.port}

            logger.info(f'Connecting to HBI pool {me.addr}')
            service_pool = MicroPool(globals(), me.addr, net_opts=me.net_opts)

            # bind to a pool proc
            hbic = await service_pool.proc(session='xxx')
            logger.info(f'Assigned HBI proc {hbic}')

            console = HBIConsole(hbic)
            console.run()

            # passively wait disconnected by service
            await hbic.wait_disconnected()

            logger.info(f'Done with HBI pool {me.addr}')
            # disconnect from pool
            service_pool.disconnect()


        asyncio.get_event_loop().run_until_complete(main())


else:
    assert False, f'Unexpected run name: {__name__!s}'
