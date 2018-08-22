"""
Micro service mode runner for HBI module

"""

import asyncio
import json
import os
import runpy
import socket
import sys
import traceback

import hbi
from hbi import me
from hbi.aio import *
from hbi.log import *
from hbi.util import *
from . import pe
from .mgmt import *

__all__ = ()

assert '__main__' == __name__

logger = get_logger(__package__)

# options with meaningful defaults
pool_size = max(1, os.cpu_count() // 2)
hot_back = 2


def print_usage():
    print(rf'''
Run HBI module in micro service mode, usage:
  python -m hbi.pool <service-module> [-n <pool-size>=[{pool_size}]] [-b <hot-back>={hot_back}]'''
          # continue a long line
          ''' [-6] [-p port] [-h host] [-s sock]'''
          rf''' [-q <rss-quota>={hrdsz(pe.worker_rss_quota)}] [-o <module-overrides>]'''
          , file=sys.stderr)


def parse_args():
    global pool_size, hot_back

    arg_i = 0
    while arg_i + 1 < len(sys.argv):
        arg_i += 1

        if '--' == sys.argv[arg_i]:
            me.argv = sys.argv[arg_i + 1:]
            break

        if '-n' == sys.argv[arg_i]:
            arg_i += 1
            pool_size = int(sys.argv[arg_i])
            continue

        if '-b' == sys.argv[arg_i]:
            arg_i += 1
            hot_back = int(sys.argv[arg_i])
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

        if '-q' == sys.argv[arg_i]:
            arg_i += 1
            pe.worker_rss_quota = hwdsz(sys.argv[arg_i])
            continue

        if '-o' == sys.argv[arg_i]:
            arg_i += 1
            me.init_globals = json.loads(sys.argv[arg_i])
            continue

        if sys.argv[arg_i].startswith('-'):
            print(f'Invalid option: {sys.argv[arg_i]}', file=sys.stderr)
            return False

        if me.modu_name is None:
            me.modu_name = sys.argv[arg_i]
            continue

        print(f'Unexpected arg: {sys.argv[arg_i]}', file=sys.stderr)
        return False

    if me.modu_name is None:
        return False

    return True


def main():
    global pool_size, hot_back

    if not parse_args():
        print_usage()
        sys.exit(1)

    try:

        if me.addr is None:
            if me.host is None:
                import platform

                me.host = platform.node()
            me.addr = {'host': me.host, 'port': me.port}
        logger.info(f'Starting HBI service pool ({hot_back}/{pool_size}) with module [{me.modu_name}] on {me.addr}')
        me.loop = asyncio.get_event_loop()

        try:

            pe.master = MicroMaster(
                pool_size, hot_back,
            )

            me.server = me.loop.run_until_complete(hbi.HBIC.create_server(
                lambda: runpy.run_module(f'{__package__}._pmgr.mcp', run_name='__hbi_pool_consumer_peer__'),
                addr=me.addr, net_opts=me.net_opts, loop=me.loop
            ))

            runpy.run_module(me.modu_name, run_name='__hbi_serving__')

            pe.master.start_pool(me.loop)

            handle_signals()
            run_aio_servers()

        finally:
            me.loop.close()
            logger.info('HBI service pool shutdown.')

    except Exception:
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


main()
sys.exit(0)
