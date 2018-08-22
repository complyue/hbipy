"""
HBI module environment

"""

# explicitly exports nothing to make it apparent that this module is not intended to be wild imported from
__all__ = ()

# shared variables amongst pool master and procs, must be transferable via dict repr
__share__ = (
    'host', 'addr', 'net_opts', 'modu_name', 'init_globals', 'argv',
)

host = None
port = 3232
addr = None
net_opts = {}

modu_name = None
init_globals = None

argv = []

loop = None

# server socket in standalone server or pool proc mode
server = None
