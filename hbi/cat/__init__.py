import logging

from ..log import hbi_root_logger

# enable debug messages for hbi
if hbi_root_logger.getEffectiveLevel() > logging.DEBUG:
    hbi_root_logger.setLevel(logging.DEBUG)
