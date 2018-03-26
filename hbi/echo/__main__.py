"""
HBI Echo server and client

"""

import logging

import hbi
from hbi import me as hbie

logger = logging.getLogger(__package__)

if '__hbi_serving__' == __name__:
    # modu run per HBI server initialization

    # hbi_host/hbi_port are available, the server has just started listening
    logger.info(f'Echo server listening {hbie.addr}')

elif '__hbi_accepting__' == __name__:
    # modu run per client HBI connection accepted at server side

    # hbi_peer is assigned after the module finished initialization, so it's okay to be used in functions defined here,
    # but during module initialization, it is None
    hbi_peer = None


    def hbi_boot():
        # running as echo server
        assert hbie.server is not None

        # if `hbi_boot` is triggered at server side, that means client modu doesn't provide an `hbi_boot`,
        # but this `hbi.echo` modu surely does. so here we know that peer is an unknown client modu,
        # just print warning message to it.
        hbi_peer.fire(r'''
import sys
print('This is echo server, merely serving `echo(*args, **kwargs)`, other code will obviously fail.', file=sys.stderr)
''')
        hbi_peer.disconnect()


    # net_info (so __str__) will be missing after disconnected, we need to save it if we want to report such info in
    # hbi_disconnected() callback.
    hbi_peer_name = 'HBI <never-connected>'


    def hbi_connected():
        global hbi_peer_name
        hbi_peer_name = str(hbi_peer)

        logger.info(f'HBI client {hbi_peer} connected.')


    def hbi_peer_done():
        logger.info(f'HBI client {hbi_peer} quited.')


    def hbi_disconnecting(err_reason=None):
        logger.warning(f'HBI client {hbi_peer} being disconnected ...')
        if err_reason is not None:
            logger.warning(f'  - Due to reason: {err_reason}')


    def hbi_disconnected(err_reason=None):
        logger.info(f'HBI client {hbi_peer_name} disconnected.')


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
    hbi_peer = None  # type: hbi.HBIC

    _land_code = False


    def hbi_boot():
        global hbi_host, hbi_port  # supplied by HBI, at both server/client side
        global hbi_argv  # supplied by HBI, from command line, whatever after --
        global hbi_server  # supplied by HBI, always be None at client side

        # running as echo client
        assert hbie.server is None

        import sys
        import threading
        from code import InteractiveConsole

        # run client repl like a console, but fire the interactive source to be landed remotely
        class HBIConsole(InteractiveConsole):

            def runsource(self, source, filename="<input>", symbol="single"):
                global _land_code  # to control local code landing

                if not hbi_peer.connected:
                    logger.warning('HBI disconnected, exiting...')
                    sys.exit(1)

                source = str(source).lstrip()
                if len(source) < 1:
                    # empty source, nop
                    return

                if '%' == source[0]:
                    # magic cmd

                    if '%land' == source.strip():
                        _land_code = True
                        logger.warning('%%% Now HBI code will be landed locally')
                        return
                    if '%noland' == source.strip():
                        _land_code = False
                        logger.warning('%%% Now HBI code will NOT be landed locally')
                        return

                    if source.startswith('%co '):
                        hbi_peer.fire_corun(source[4:])
                        return

                    logger.error(f'No such magic: {source}')

                hbi_peer.fire(source)

        console = HBIConsole()
        sys.ps1 = 'hbi> '

        def console_session():
            global hbi_disconnected

            err_disconnect, err_stack = None, None
            try:
                console.interact(fr'''
HBI connected {hbi_peer.net_info}

                            -==[ WARNING ]==- 
!!! ANY code you submit here will be executed by the server, take care !!!
                            -==[ WARNING ]==- 

&&& Now HBI code will{'' if _land_code else ' NOT'} be landed, 
&&& you can control local landing with %land and %noland magic commands.

''', r'''
Bye.
''')

            except (SystemExit, KeyboardInterrupt):
                # upon interpreter exit, unset the disconnection callback to treat further disconnection as expected
                hbi_disconnected = None
            except Exception as exc:
                import traceback
                err_disconnect = exc
                err_stack = traceback.format_exc()
            hbi_peer.disconnect(err_disconnect, err_stack, try_send_peer_err=False)

        # main thread must run hbi loop, the repl has to run in a separate thread
        th = threading.Thread(target=console_session)
        th.start()


    def hbi_disconnected(err_reason=None):
        # defined here to handle unexpected disconnection
        import sys
        if err_reason is None:
            logger.error('HBI connection closed by peer.')
        else:
            logger.error(rf'''
HBI connection closed by peer due to error:
{err_reason}
''')
        sys.exit(1)


    def __hbi_land__(code, wire_dir):
        # this magic method if defined, hijacks code received over HBI wire for local execution (landing)

        print(rf'''
-== CODE TO LAND ==-
[#{wire_dir}]{code}
====================
''', flush=True)

        if _land_code:
            # perform normal landing, i.e. to execute it locally
            return NotImplemented

else:
    assert False, 'hbi.echo is only supposed to run as an HBI server or client module.'
