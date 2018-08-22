"""
HBI pool service proc environment

"""

# explicitly exports nothing to make it apparent that this module is not intended to be wild imported from
__all__ = ()

# shared variables amongst pool master and procs, must be transferable via dict repr
__share__ = (
    'worker_rss_quota',
)

worker_rss_quota = 2 * 1024 ** 3  # 2 GB by default

# session id str of current proc
session = None
# whether current proc worker subprocess should retire
retiring = False
# a future indicating service task(s) pending completed
service_wip = None
