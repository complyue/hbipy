"""
HBI cat

"""

import logging

logger = logging.getLogger(__package__)

assert '__hbi_connecting__' == __name__, 'hbi.cat is only supposed to run as an HBI client module.'


def __hbi_land__(code, wire_dir):
    logger.info(rf'''
-== CODE TO LAND ==-
[#{wire_dir}]{code}
====================
''')


def hbi_disconnected(err_reason=None):
    import sys
    if err_reason is None:
        logger.info('HBI connection closed by peer.')
        sys.exit(0)
    else:
        logger.error(rf'''
HBI connection closed by peer due to error:
{err_reason}
''')
        sys.exit(1)
