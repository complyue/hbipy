import sys
import threading
from code import InteractiveConsole

import hbi
from hbi.log import get_logger

__all__ = [
    'HBIConsole',
]

logger = get_logger(__name__)


class HBIConsole(InteractiveConsole):
    """
    run client repl like a console, but fire the interactive source to be landed remotely

    """

    def __init__(self, hbi_peer: hbi.HBIC):
        super().__init__()

        self.hbi_peer = hbi_peer
        self.running = False
        self.land_code = False  # to control local code landing

    def runsource(self, source, filename="<input>", symbol="single"):
        if not self.hbi_peer.connected:
            logger.warning('HBI disconnected, exiting...')
            sys.exit(1)

        source = str(source).lstrip()
        if len(source) < 1:
            # empty source, nop
            return

        if '%' == source[0]:
            # magic cmd

            if '%land' == source.strip():
                self.land_code = True
                logger.warning('%%% Now HBI code will be landed locally')
                return
            if '%noland' == source.strip():
                self.land_code = False
                logger.warning('%%% Now HBI code will NOT be landed locally')
                return

            if source.startswith('%co '):
                self.hbi_peer.fire_corun(source[4:])
                return

            logger.error(f'No such magic: {source}')

        self.hbi_peer.fire(source)

    def console_session(self):
        err_disconnect, err_stack = None, None
        try:
            self.interact(fr'''
HBI connected {self.hbi_peer.net_info}

                    -==[ WARNING ]==- 
!!! ANY code you submit here will be executed by the server, take care !!!
                    -==[ WARNING ]==- 

&&& Now HBI code will{'' if self.land_code else ' NOT'} be landed, 
&&& you can control local landing with %land and %noland magic commands.

''', r'''
Bye.
''')

        except (SystemExit, KeyboardInterrupt):
            # while shell not running, further disconnection is expected
            self.running = False
        except Exception as exc:
            import traceback
            err_disconnect = exc
            err_stack = traceback.format_exc()
        self.hbi_peer.disconnect(err_disconnect, err_stack, try_send_peer_err=False)

    def run(self):
        self.running = True
        sys.ps1 = 'hbi> '

        # main thread must run hbi loop, the repl has to run in a separate thread
        th = threading.Thread(target=self.console_session)
        th.start()
