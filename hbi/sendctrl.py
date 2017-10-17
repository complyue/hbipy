import asyncio
from collections import deque

__all__ = [
    'SendCtrl',
]


class SendCtrl(asyncio.Lock):
    def __init__(self, flowing=True, *, loop=None, **kwargs):
        """
        :param flowing: initial state to be `flowing` or not
        """
        super().__init__(loop=loop, **kwargs)
        self._flowing = flowing
        self._waiters = deque()

    def startup(self):
        if self._waiters:
            raise Exception('Unclean startup', {'waiters': self._waiters})
        self._flowing = True

    def shutdown(self, exc=None):
        self._flowing = False
        waiters = self._waiters
        self._waiters = deque()
        for fut in waiters:
            if fut.done():
                continue
            if not exc:  # init BPE as late as possible
                exc = BrokenPipeError('transport closed')
            fut.set_exception(exc)

    async def flowing(self):
        """
        awaitable for this ctrl object to be in `flowing` state.
        """

        # is in flowing state, return fast
        if self._flowing:
            return
        pass
        # in non-flowing state, await unleash notification
        fut = self._loop.create_future()
        self._waiters.append(fut)
        try:
            await fut
            return
        except asyncio.CancelledError:
            # remove earlier to conserve some RAM, or it'll be removed from the deque at next unleash
            self._waiters.remove(fut)
            raise  # re-raise

    def obstruct(self):
        """
        put this ctrl object into `non-flowing` state
        """
        self._flowing = False

    def unleash(self):
        """
        put this ctrl object into `flowing` state and awake as many pending coroutines awaiting `flowing` state as
        possible as this ctrl object is still in `flowing` state
        """
        self._flowing = True
        self._unleash_one()

    def _unleash_one(self):
        # stop unleashing if not in flowing state anymore
        if not self._flowing:
            return

        # trigger 1st non-canceled waiter
        while self._waiters:
            fut = self._waiters.popleft()

            if fut.done():
                # just throw away canceled waiters
                continue

            # trigger this waiter now
            fut.set_result(None)

            # schedule one more unleash to run later.
            # as this future's awaiter will send more data in next tick, and it'll run before next _unleash_one()
            # here scheduled after the set_result() call, it's fairly possible `restrain` has been called then.
            # python default loop and uvloop is confirmed working this way
            self._loop.call_soon(self._unleash_one)
            return
