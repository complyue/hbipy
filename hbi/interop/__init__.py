# this module meant to be imported by modules serving as HBI contexts, to provide adaptive behaviors across different
# languages/runtimes of the hosting environment

from math import nan

__all__ = [
    'null', 'true', 'false', 'nan', 'NaN',
]

null = None
true = True
false = False
NaN = nan
