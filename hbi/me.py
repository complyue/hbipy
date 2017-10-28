"""
HBI server/client module environment

This module provides environment for HBI modules run by: `python -m hbi <modu> ...`

"""

# explicitly exports nothing to make it apparent that this module is not intended to be import * from
__all__ = []

host = None
port = 3232

modu_name = None

argv = []

loop = None
server = None
