import asyncio
from concurrent import futures

__all__ = ["run_aio_servers"]


def run_aio_servers():
    loop = asyncio.get_event_loop()
    try:
        loop.run_forever()
    except (KeyboardInterrupt, asyncio.CancelledError, futures.CancelledError):
        pass
    loop.run_until_complete(loop.shutdown_asyncgens())
