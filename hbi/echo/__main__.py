"""

"""

import hbi

# _server_ is assigned at server side on each client connection
_server_ = None  # type:

# _peer_ is assigned on client/server HBIC construction
_peer_ = None  # type: hbi.HBIC


def hbi_boot(hbic=None):
    import sys
    import threading
    from code import InteractiveConsole

    if _server_ is not None:
        # run as echo server
        # if `hbi_boot` is triggered at server side, that means client modu doesn't provide a `hbi_boot`,
        # but this modu if run as client surely does. so here we assume unknown client modu is hooked,
        # just print warning message to it.
        _peer_.fire(r'''
import sys
print('This is echo server, merely serving `echo(*args, **kwargs)`, other code will obviously fail.', file=sys.stderr)
''')
        return

    # run client repl like a console, but fire the input source to be landed remotely
    # TODO find ways to detect hbi disconnection during repl reading-in

    if hbic is None:
        hbic = _peer_

    class HBIConsole(InteractiveConsole):

        def runsource(self, source, filename="<input>", symbol="single"):
            if not hbic.connected:
                print('HBI disconnected, exiting...', file=sys.stderr)
                raise SystemExit
            hbic.fire(source)

    console = HBIConsole()
    sys.ps1 = 'hbi> '

    def console_session():
        try:
            console.interact(fr'''
HBI connected {hbic.net_info}
Warning: 
  !!! ANY code you submit here will be executed by the server, take care !!!
'''
                             )
        except SystemExit:
            pass
        hbic.disconnect()
        print('Bye.', file=sys.stderr)

    th = threading.Thread(target=console_session)
    th.start()


def echo(*args, **kwargs):
    _peer_.fire(
        rf'''
print(*({args!r}), flush=True, **({kwargs!r}))
'''
    )
