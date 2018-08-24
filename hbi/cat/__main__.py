"""
HBI cat

"""

import asyncio
import logging
import sys

from hbi import me
from hbi.pool import MicroPool

logger = logging.getLogger(__package__)


def __hbi_land__(code, wire_dir):
    logger.info(rf'''
-== CODE TO LAND ==-
[#{wire_dir}]{code}
====================
''')


def hbi_disconnected(err_reason=None):
    if err_reason is None:
        logger.info('HBI connection closed by peer.')
        sys.exit(0)
    else:
        logger.error(rf'''
HBI connection closed by peer due to error:
{err_reason}
''')
        sys.exit(1)


if '__hbi_connecting__' == __name__:
    # run in standalone client mode, hbi cmdl handles all
    pass

elif '__main__' == __name__:
    # run in utility module mode, acting as micro service consumer

    def print_usage():
        print(r'''
HBI service pool simple tact utility, usage:
  python -m hbi.cat [-6] [-p port] [-h host] [-s sock]
''', file=sys.stderr)


    async def main():
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

        # sending boot request in burst mode
        await hbic.send_code('hbi_boot()')

        # passively wait disconnected by service
        await hbic.wait_disconnected()

        logger.info(f'Done with HBI pool {me.addr}')
        # disconnect from pool
        service_pool.disconnect()


    asyncio.get_event_loop().run_until_complete(main())


else:
    assert False, f'Unexpected run name: {__name__!s}'
