"""
Hosting Based Interfacing

    HBI is an alternative idiomatic methodology to implement IPC (inter-process communication), it tends to be
    adapting amongst RPC(remote procedure call), Streaming/Pipelining, and PubSub/Notification styles of communication
    at runtime, and typically leverage a single network channel (sockets, pipes, http, websockets, etc.) between two
    peer nodes.

"""
from .aio import *
from .buflist import *
from .bytesbuf import *
from .co import *
from .conn import *
from .context import *
from .ho import *
from .interop import *
from .log import *
from .po import *
from .pool import *
from .proto import *
from .sendctrl import *
from .shell import *
from .util import *
from .version import *

__all__ = [

    # exports from .aio
    'run_aio_servers', 'handle_signals',

    # exports from .buflist
    'BufferList',

    # exports from .bytesbuf
    'BytesBuffer',

    # exports from .co
    'Conver',

    # exports from .conn
    'HBIC', 'HBIS',

    # exports from .context
    'run_in_context',

    # exports from .ho
    'HostingEnd',

    # exports from .interop
    'null', 'true', 'false', 'nan', 'NaN', 'JSOND',

    # exports from .log
    'hbi_root_logger', 'get_logger',

    # exports from .po
    'PostingEnd',

    # exports from .pool
    'ServiceMaster', 'PoolMaster', 'ProcWorker', 'ServiceConsumer',

    # exports from .proto
    'PACK_HEADER_MAX',

    # exports from .sendctrl
    'SendCtrl',

    # exports from .shell
    'HBIConsole',

    # exports from .util
    'hrdsz', 'hwdsz',

    # exports from .version
    'version',

]
