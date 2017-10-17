import asyncio
import runpy
import sys

import hbi

assert __name__.endswith('__main__')


def print_usage():
    print('HBI module runner, usage:\n  python -m hbi [-l] [-p port] [-h host] <module>', file=sys.stderr)


host = None
port = 3232
modu_name = None

hbi_argv = []


def main():
    global host, port, modu_name, hbi_argv

    if len(sys.argv) <= 1:
        return print_usage()

    run_server = False

    arg_i = 0
    while arg_i + 1 < len(sys.argv):
        arg_i += 1

        if '--' == sys.argv[arg_i]:
            hbi_argv = sys.argv[arg_i + 1:]
            break

        if '-l' == sys.argv[arg_i]:
            run_server = True
            continue

        if '-p' == sys.argv[arg_i]:
            arg_i += 1
            port = int(sys.argv[arg_i])
            continue

        if '-h' == sys.argv[arg_i]:
            arg_i += 1
            host = sys.argv[arg_i]
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

        hbis = None

        def ctx_factory():
            nonlocal hbis

            ctx = runpy.run_module(modu_name, {
                'hbi_host': host, 'hbi_port': port, 'hbi_argv': hbi_argv,
                'hbi_server': hbis, 'hbi_peer': None,
            }, run_name='__hbi_accepting__')
            return ctx

        print(f'Starting HBI server with module {modu_name} on {host or "*"}:{port}', file=sys.stderr)
        hbis = loop.run_until_complete(hbi.HBIC.create_server(ctx_factory, addr={
            'host': host, 'port': port,
        }, loop=loop))
        try:
            runpy.run_module(modu_name, {
                'hbi_host': host, 'hbi_port': port, 'hbi_argv': hbi_argv,
                'hbi_server': hbis, 'hbi_peer': None,
            }, run_name='__hbi_serving__')
            loop.run_until_complete(hbis.wait_closed())
        except KeyboardInterrupt:
            return

    else:

        if host is None:
            return print_usage()

        print(f'Connecting to HBI server {host}:{port} with module {modu_name}', file=sys.stderr)
        try:
            ctx = runpy.run_module(modu_name, {
                'hbi_host': host, 'hbi_port': port, 'hbi_argv': hbi_argv,
                'hbi_server': None, 'hbi_peer': None,
            }, run_name='__hbi_connecting__')

            try:
                hbic = hbi.HBIC(ctx, addr={
                    'host': host, 'port': port,
                }, loop=loop)
                hbic.run_until_connected()
                print(f'Connected {hbic}', file=sys.stderr)
            except OSError as exc:
                print(f'Connection failed: {exc}', file=sys.stderr)
                sys.exit(1)

            hbi_boot = ctx.get('hbi_boot', None)
            if hbi_boot is not None:

                # calling client side boot function
                hbi_boot()

            else:

                # sending boot request to server
                hbic.fire('hbi_boot()')

            hbic.run_until_disconnected()

        except KeyboardInterrupt:
            pass


main()
