# one instance of this module will be created for each HBIC by runpy.run_module()
# so variables defined in this module are independent copies between HBI connections.
# globally shared states should be defined as variables in other normal modules, those modules can be imported
# here for access

import hbi

# _peer_ is actually assigned to reference the hbic at runtime when landing flied code
_peer_ = None  # type: hbi.HBIC


def hbi_boot(hbic=None):
    import sys
    import threading
    from code import InteractiveConsole

    if hbic is None:
        hbic = _peer_

    hbic.fire('''
from datetime import datetime
echo(f'This is server, got your msg at {datetime.now()}', flush=True)
''')

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
            console.interact('HBI connected %r' % (hbic.net_info,))
        except SystemExit:
            pass
        hbic.disconnect()
        print('Bye.', file=sys.stderr)

    th = threading.Thread(target=console_session)
    th.start()


def echo(*args, **kwargs):
    _peer_.fire(
        '''
print(*(%r), **(%r))
''' % (args, kwargs)
    )
