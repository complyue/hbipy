"""
HBI Echo server and client

"""

if '__hbi_serving__' == __name__:
    # modu run per HBI server initialization

    # _server_/_host_/_port_ are available, _server_ has just started listening
    print(f'Echo server listening {_host_}:{_port_}')

else:
    # modu run per HBI connection

    # _server_ is assigned at server side on each client connection
    _server_ = None

    # _peer_ is assigned on client/server HBIC construction
    _peer_ = None


    def hbi_boot():
        import sys
        import threading
        from code import InteractiveConsole

        if _server_ is not None:
            # running as echo server
            # if `hbi_boot` is triggered at server side, that means client modu doesn't provide an `hbi_boot`,
            # but this `hbi.echo` modu surely does. so here we know that peer is an unknown client modu,
            # just print warning message to it.
            _peer_.fire(r'''
import sys
print('This is echo server, merely serving `echo(*args, **kwargs)`, other code will obviously fail.', file=sys.stderr)
''')
            return

        # run client repl like a console, but fire the interactive source to be landed remotely

        class HBIConsole(InteractiveConsole):
            # TODO find ways to detect hbi disconnection during repl reading-in

            def runsource(self, source, filename="<input>", symbol="single"):
                if not _peer_.connected:
                    print('HBI disconnected, exiting...', file=sys.stderr)
                    raise SystemExit
                _peer_.fire(source)

        console = HBIConsole()
        sys.ps1 = 'hbi> '

        def console_session():
            try:
                console.interact(fr'''
HBI connected {_peer_.net_info}
Warning: 
  !!! ANY code you submit here will be executed by the server, take care !!!
''')
            except SystemExit:
                pass

            _peer_.disconnect()
            print('Bye.', file=sys.stderr)

        # main thread must run hbi loop, the repl has to run in a separate thread
        th = threading.Thread(target=console_session)
        th.start()


    def echo(*args, **kwargs):
        _peer_.fire(rf'''
print('\n--BEGIN-REMOTE-MSG--')
print(*({args!r}), flush=True, **({kwargs!r}))
print('--END-REMOTE-MSG--')
''')
