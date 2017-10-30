import asyncio
import logging
import runpy
import socket
import sys

import hbi
from . import me as hbie

logger = logging.getLogger(__package__)

assert '__main__' == __name__


def print_usage():
    print(r'''
HBI server/client module runner, usage:
  python -m hbi [-l] [-6] [-p port] [-h host] [-s sock] <module>
''', file=sys.stderr)


def main():
    if len(sys.argv) <= 1:
        return print_usage()

    run_server = False
    net_opts = {}

    arg_i = 0
    while arg_i + 1 < len(sys.argv):
        arg_i += 1

        if '--' == sys.argv[arg_i]:
            hbie.hbi_argv = sys.argv[arg_i + 1:]
            break

        if '-l' == sys.argv[arg_i]:
            run_server = True
            continue

        if '-6' == sys.argv[arg_i]:
            net_opts['family'] = socket.AF_INET6
            continue

        if '-p' == sys.argv[arg_i]:
            arg_i += 1
            hbie.port = int(sys.argv[arg_i])
            continue

        if '-h' == sys.argv[arg_i]:
            arg_i += 1
            hbie.host = sys.argv[arg_i]
            continue

        if '-s' == sys.argv[arg_i]:
            arg_i += 1
            hbie.addr = sys.argv[arg_i]
            continue

        if sys.argv[arg_i].startswith('-'):
            return print_usage()

        if hbie.modu_name is None:
            hbie.modu_name = sys.argv[arg_i]
            continue

        return print_usage()

    if hbie.modu_name is None:
        return print_usage()

    hbie.loop = asyncio.get_event_loop()

    if run_server:

        if hbie.addr is None:
            if hbie.host is None:
                import platform
                hbie.host = platform.node()
            hbie.addr = {'host': hbie.host, 'port': hbie.port}

        logger.info(f'Starting HBI server with module {hbie.modu_name} on {hbie.host or "*"}:{hbie.port}')
        hbie.server = hbie.loop.run_until_complete(hbi.HBIC.create_server(
            lambda: runpy.run_module(hbie.modu_name, run_name='__hbi_accepting__'),
            addr=hbie.addr, net_opts=net_opts, loop=hbie.loop
        ))
        try:
            runpy.run_module(hbie.modu_name, run_name='__hbi_serving__')
            hbie.loop.run_until_complete(hbie.server.wait_closed())
        except KeyboardInterrupt:
            logger.info(f'HBI shutting down in responding to Ctrl^C ...')
            return
        else:
            logger.info(f'HBI server closed, shutting down ...')
        finally:
            hbie.loop.run_until_complete(hbie.loop.shutdown_asyncgens())
            hbie.loop.close()
            logger.info('HBI shutdown.')

    else:

        if hbie.addr is None:
            if hbie.host is None:
                return print_usage()
            hbie.addr = {'host': hbie.host, 'port': hbie.port}

        logger.info(f'Connecting to HBI server {hbie.addr} with module {hbie.modu_name}')
        hbic = None
        try:
            try:
                hbic = hbi.HBIC(
                    runpy.run_module(hbie.modu_name, run_name='__hbi_connecting__'),
                    addr=hbie.addr, net_opts=net_opts, loop=hbie.loop
                )
                hbic.run_until_connected()
                logger.info(f'HBI connected {hbic}')
            except OSError as exc:
                logger.warning(f'HBI connection failed: {exc}')
                sys.exit(1)

            hbi_boot = hbic.context.get('hbi_boot', None)
            if hbi_boot is not None:
                # calling client side boot function
                hbi_boot()
            else:
                # sending boot request to server
                hbic.fire('hbi_boot()')
            hbic.run_until_disconnected()
        except KeyboardInterrupt:
            if hbic is not None:
                logger.warning('HBI terminating on Ctrl^C ...')
                hbic.disconnect()
        finally:
            logger.info(f'HBI connection closed, shutting down ...')
            hbie.loop.run_until_complete(hbie.loop.shutdown_asyncgens())
            hbie.loop.close()
            logger.info('HBI shutdown.')


main()
