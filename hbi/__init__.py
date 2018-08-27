"""
Hosting Based Interfacing

    HBI is an alternative idiomatic methodology to implement IPC (inter-process communication), it tends to be
    adapting amongst RPC(remote procedure call), Streaming/Pipelining, and PubSub/Notification styles of communication
    at runtime, and typically leverage a single network channel (sockets, pipes, http, websockets, etc.) between two
    peer nodes.

"""

from .conn import *
from .context import *
from .pool.consumer import *
from .sockconn import *
from .version import version as __version__

__all__ = [
    '__version__',

    'HBIC', 'ServiceMaster', 'run_in_context',
]
