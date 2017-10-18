import logging

# enable debug messages for hbi
hrl = logging.getLogger("hbi")
if hrl.level > logging.DEBUG:
    hrl.setLevel(logging.DEBUG)
