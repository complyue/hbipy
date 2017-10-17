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

    active_exiting = False


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
            _peer_.disconnect()
            return

        # run client repl like a console, but fire the interactive source to be landed remotely

        class HBIConsole(InteractiveConsole):

            def runsource(self, source, filename="<input>", symbol="single"):

                if not _peer_.connected:
                    print('HBI disconnected, exiting...', file=sys.stderr)
                    raise SystemExit

                if len(source) <= 0:
                    # empty source, nop
                    return

                _peer_.fire(source)

        console = HBIConsole()
        sys.ps1 = 'hbi> '

        def console_session():
            global active_exiting

            try:
                console.interact(fr'''
HBI connected {_peer_.net_info}

                            -==[ WARNING ]==- 
!!! ANY code you submit here will be executed by the server, take care !!!
''', r'''
Bye.
''')
                active_exiting = True
                _peer_.disconnect()
            except SystemExit:
                pass

        # main thread must run hbi loop, the repl has to run in a separate thread
        th = threading.Thread(target=console_session)
        th.start()


    def hbi_disconnected():
        global active_exiting

        import sys
        if not active_exiting:
            print('HBI connection closed by peer.', flush=True, file=sys.stderr)
        sys.exit(1)


    def echo(*args, **kwargs):
        _peer_.fire(rf'''
print('\n--BEGIN-REMOTE-MSG--')
print(*({args!r}), **({kwargs!r}))
print('---END--REMOTE-MSG--', flush=True)
''')
