# this module meant to be imported by modules serving as HBI contexts,
# to provide adaptive behaviors across different languages/runtimes
# of the hosting environment

import json
from math import nan

__all__ = ["null", "true", "false", "nan", "NaN", "JSOND"]

null = None
true = True
false = False
NaN = nan


class JSOND(dict):
    """
    A subclass of standard python dict, with repr in json

    """

    def __repr__(self):
        return json.dumps(self)

    __str__ = __repr__
