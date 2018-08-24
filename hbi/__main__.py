"""
Adhoc mode runner for HBI module

"""

import asyncio
import logging
import runpy
import socket
import sys

import hbi

from . import me

__all__ = ()

assert '__main__' == __name__

logger = logging.getLogger(__package__)


def print_usage():
    print(r'''
Run HBI module in adhoc server or client mode, usage:
  python -m hbi <module> [-l] [-6] [-p port] [-h host] [-s sock]
''', file=sys.stderr)


def main():
    if len(sys.argv) <= 1:
        return print_usage()

    run_server = False

    arg_i = 0
    while arg_i + 1 < len(sys.argv):
        arg_i += 1

        if '--' == sys.argv[arg_i]:
            me.argv = sys.argv[arg_i + 1:]
            break

        if '-l' == sys.argv[arg_i]:
            run_server = True
            continue

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

        if me.modu_name is None:
            me.modu_name = sys.argv[arg_i]
            continue

        return print_usage()

    if me.modu_name is None:
        return print_usage()

    me.loop = asyncio.get_event_loop()

    if run_server:

        if me.addr is None:
            if me.host is None:
                # import platform
                # me.host = platform.node()
                me.host = '127.0.0.1'
            me.addr = {'host': me.host, 'port': me.port}

        logger.info(f'Starting HBI server with module {me.modu_name} on {me.addr}')
        me.server = me.loop.run_until_complete(hbi.HBIC.create_server(
            lambda: runpy.run_module(me.modu_name, run_name='__hbi_accepting__'),
            addr=me.addr, net_opts=me.net_opts, loop=me.loop
        ))
        try:
            runpy.run_module(me.modu_name, run_name='__hbi_serving__')
            me.loop.run_until_complete(me.server.wait_closed())
        except KeyboardInterrupt:
            logger.info(f'HBI shutting down in responding to Ctrl^C ...')
            return
        else:
            logger.info(f'HBI server closed, shutting down ...')
        finally:
            me.loop.run_until_complete(me.loop.shutdown_asyncgens())
            me.loop.close()
            logger.info('HBI shutdown.')

    else:

        if me.addr is None:
            if me.host is None:
                return print_usage()
            me.addr = {'host': me.host, 'port': me.port}

        logger.info(f'Connecting to HBI server {me.addr} with module {me.modu_name}')
        hbic = None
        try:
            try:
                hbic = hbi.HBIC(
                    runpy.run_module(me.modu_name, run_name='__hbi_connecting__'),
                    addr=me.addr, net_opts=me.net_opts, loop=me.loop
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
            me.loop.run_until_complete(me.loop.shutdown_asyncgens())
            me.loop.close()
            logger.info('HBI shutdown.')


main()
