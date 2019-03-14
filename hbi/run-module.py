#!/usr/bin/env python
"""
helper script to assist pydev debuggers when target module (pkg/__main__.py) meant to be started with `python -m`

"""

import os
import sys
import traceback

if __name__ == "__main__":
    import runpy

    if len(sys.argv) < 2:
        raise RuntimeError("module name is required")

    try:
        modu_name = sys.argv[1]
        sys.argv = sys.argv[
            1:
        ]  # shift off run-module.py, argv[0] will be replaced by module's __main__
        runpy.run_module(modu_name, run_name=__name__, alter_sys=True)
    except NameError:
        print("sys.path => %r" % sys.path, file=sys.stderr)
        raise
    except Exception:
        traceback.print_exc(file=sys.stderr)
        raise
else:
    print(
        " ** Forked pid=%r __name__=%r by ppid=%r"
        % (os.getpid(), __name__, os.getppid()),
        file=sys.stderr,
        flush=True,
    )
