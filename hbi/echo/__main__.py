"""
HBI Echo server and client

"""

if '__hbi_serving__' == __name__:
    # modu run per HBI server initialization

    # hbi_host/hbi_port are available, the server has just started listening
    print(f'Echo server listening {hbi_host}:{hbi_port}')

elif '__hbi_accepting__' == __name__:
    # modu run per client HBI connection accepted at server side

    # hbi_peer is assigned after the module finished initialization, so it's okay to be used in functions defined here,
    # but during module initialization, it is None
    hbi_peer = None


    def hbi_boot():
        global hbi_host, hbi_port  # supplied by HBI, at both server/client side
        global hbi_argv  # supplied by HBI, from command line, whatever after --
        global hbi_server  # supplied by HBI, always be None at client side

        # running as echo server
        assert hbi_server is not None

        # if `hbi_boot` is triggered at server side, that means client modu doesn't provide an `hbi_boot`,
        # but this `hbi.echo` modu surely does. so here we know that peer is an unknown client modu,
        # just print warning message to it.
        hbi_peer.fire(r'''
import sys
print('This is echo server, merely serving `echo(*args, **kwargs)`, other code will obviously fail.', file=sys.stderr)
''')
        hbi_peer.disconnect()


    def hbi_peer_done():
        print(f'Client {hbi_peer} quited.', flush=True, file=sys.stderr)


    def hbi_disconnecting(err_reason=None):
        print(f'Client {hbi_peer} disconnecting.', flush=True, file=sys.stderr)
        if err_reason is not None:
            print(f'  - Due to reason: {err_reason}', flush=True, file=sys.stderr)


    def echo(*args, **kwargs):
        # assuming peer is Python interpreter for now, language adaptive mechanisms could be added

        hbi_peer.fire(rf'''
print('\n--BEGIN-REMOTE-MSG--')
print(*({args!r}), **({kwargs!r}))
print('---END--REMOTE-MSG--', flush=True)
''')

elif '__hbi_connecting__' == __name__:
    # modu run per client HBI connection

    # hbi_peer is assigned after the module finished initialization, so it's okay to be used in functions defined here,
    # but during module initialization, it is None
    hbi_peer = None


    def hbi_boot():
        global hbi_host, hbi_port  # supplied by HBI, at both server/client side
        global hbi_argv  # supplied by HBI, from command line, whatever after --
        global hbi_server  # supplied by HBI, always be None at client side

        # running as echo client
        assert hbi_server is None

        import sys
        import threading
        from code import InteractiveConsole

        # run client repl like a console, but fire the interactive source to be landed remotely
        class HBIConsole(InteractiveConsole):

            def runsource(self, source, filename="<input>", symbol="single"):

                if not hbi_peer.connected:
                    print('HBI disconnected, exiting...', flush=True, file=sys.stderr)
                    raise SystemExit

                if len(source) <= 0:
                    # empty source, nop
                    return

                hbi_peer.fire(source)

        console = HBIConsole()
        sys.ps1 = 'hbi> '

        def console_session():
            global hbi_disconnected

            try:
                console.interact(fr'''
HBI connected {hbi_peer.net_info}

                            -==[ WARNING ]==- 
!!! ANY code you submit here will be executed by the server, take care !!!
                            -==[ WARNING ]==- 
''', r'''
Bye.
''')
                # actively disconnecting, clear the disconnection callback
                hbi_disconnected = lambda: None

                hbi_peer.disconnect()
            except SystemExit:
                pass

        # main thread must run hbi loop, the repl has to run in a separate thread
        th = threading.Thread(target=console_session)
        th.start()


    def hbi_disconnected():
        # defined here to handle unexpected disconnection

        import sys
        print('HBI connection closed by peer.', flush=True, file=sys.stderr)
        sys.exit(1)


    def __hbi_land__(code, wire_dir):
        # this magic method if defined, hijacks code received over HBI wire for local execution (landing)

        print(rf'''
-== LANDING CODE ==-
[#{wire_dir}]{code}
====================
''', flush=True)

        # still perform normal landing
        return NotImplemented

else:
    assert False, f'Unexpected HBI module run name: {__name__} ?!'
