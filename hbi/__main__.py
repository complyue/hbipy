import asyncio
import runpy
import sys

import hbi

assert '__main__' == __name__


def print_usage():
    print('HBI server starter, usage:\n  python -m hbi [-p port] [-h host] <module>', file=sys.stderr)


host = None
port = 3232
modu_name = None


def main():
    global host, port, modu_name

    if len(sys.argv) <= 1:
        return print_usage()

    run_server = False

    arg_i = 0
    while arg_i + 1 < len(sys.argv):
        arg_i += 1

        if '-p' == sys.argv[arg_i]:
            arg_i += 1
            port = int(sys.argv[arg_i])
            continue

        if '-h' == sys.argv[arg_i]:
            arg_i += 1
            host = sys.argv[arg_i]
            continue

        if '-l' == sys.argv[arg_i]:
            run_server = True
            arg_i += 1
            continue

        if sys.argv[arg_i].startswith('-'):
            return print_usage()

        if modu_name is None:
            modu_name = sys.argv[arg_i]
            continue

        return print_usage()

    if modu_name is None:
        return print_usage()

    loop = asyncio.get_event_loop()

    if run_server:

        def ctx_factory():
            return runpy.run_module(modu_name)

        print(f'Starting HBI server with module {modu_name} on {host or "*"}:{port}', file=sys.stderr)
        hbis = loop.run_until_complete(hbi.HBIC.create_server(ctx_factory, addr={
            'host': host, 'port': port,
        }, loop=loop))
        try:
            loop.run_until_complete(hbis.wait_closed())
        except KeyboardInterrupt:
            return

    else:

        if host is None:
            return print_usage()

        print(f'Connecting to HBI server {host}:{port} with module {modu_name}', file=sys.stderr)
        ctx = runpy.run_module(modu_name)
        hbic = hbi.HBIC(ctx, addr={
            'host': host, 'port': port,
        }, loop=loop)
        hbic.connect()

        hbi_boot = ctx.get('hbi_boot', None)
        if hbi_boot is not None:

            # calling client side boot function
            hbi_boot(hbic)

        else:

            # sending boot request to server
            hbic.fire('hbi_boot()')

        try:
            hbic.run_until_disconnected()
        except KeyboardInterrupt:
            return


main()
