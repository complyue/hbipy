import asyncio

__all__ = ["chain_future"]


def chain_future(from_fut, to_fut):
    assert asyncio.isfuture(from_fut)

    if to_fut.done():
        # target already cancelled etc.
        return

    def from_fut_cb(fut):

        if to_fut.done():
            # target already cancelled etc.
            return

        if fut.cancelled():
            to_fut.cancel()
        elif fut.exception() is not None:
            to_fut.set_exception(fut.exception())
        else:
            fr = fut.result()
            if asyncio.isfuture(fr):
                # continue the from_fut chain
                chain_future(fr, to_fut)
            else:
                # just resolved
                to_fut.set_result(fr)

    asyncio.ensure_future(from_fut).add_done_callback(from_fut_cb)

