"""
HBI cat

"""

import logging

logger = logging.getLogger(__package__)

assert '__hbi_serving__' != __name__, 'running hbi.cat as HBI server ?! not supposedly right.'


def __hbi_land__(code, wire_dir):
    logger.info(rf'''
-== CODE TO LAND ==-
[#{wire_dir}]{code}
====================
''')


def hbi_disconnected():
    import sys
    logger.info('HBI connection closed by peer.')
    sys.exit(0)
