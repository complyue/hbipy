import asyncio

__all__ = ["CancellableQueue"]


class CancellableQueue(asyncio.Queue):
    """
    """

    def _cancel(self, waiters, exc=None):
        if exc is None:
            exc = asyncio.CancelledError("Queue cancelled")
        elif isinstance(exc, str):
            exc = asyncio.CancelledError(exc)

        while waiters:
            waiter = waiters.popleft()
            if not waiter.done():
                waiter.set_exception(exc)

    def cancel_getters(self, exc=None):
        self._cancel(self._getters, exc)

    def cancel_putters(self, exc=None):
        self._cancel(self._putters, exc)

    def cancell_all(self, exc=None):
        self._cancel(self._getters, exc)
        self._cancel(self._putters, exc)
