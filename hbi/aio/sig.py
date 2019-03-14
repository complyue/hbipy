import signal
import sys
import traceback

from ..log import *

__all__ = ["handle_signals"]

logger = get_logger(__name__)


def log_and_ignore(signum, frame):
    logger.error(f"Broken pipe!")
    traceback.print_stack(frame, file=sys.stderr)
    assert signum in {signal.SIGPIPE}


def handle_signals():
    if hasattr(signal, "SIGPIPE"):
        signal.signal(signal.SIGPIPE, log_and_ignore)
