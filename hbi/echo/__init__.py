import logging
import sys

# enable debug messages for hbi
hrl = logging.getLogger("hbi")
if hrl.getEffectiveLevel() > logging.DEBUG:
    hrl.setLevel(logging.DEBUG)
if len(hrl.handlers) < 1:
    hrl.addHandler(logging.StreamHandler(sys.stderr))
