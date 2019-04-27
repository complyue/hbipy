__all__ = ["cast_to_src_buffer", "cast_to_tgt_buffer"]


def cast_to_src_buffer(boc):
    if isinstance(boc, (bytes, bytearray)):
        # it's a bytearray
        return boc
    if not isinstance(boc, memoryview):
        try:
            boc = memoryview(boc)
        except TypeError:
            return None
    # it's a memoryview now
    if boc.nbytes == 0:  # if zero-length, replace with empty bytes
        # coz when a zero length ndarray is viewed, cast/send will raise while not needed at all
        return b""
    elif boc.itemsize != 1:
        return boc.cast("B")
    return boc


def cast_to_tgt_buffer(boc):
    if isinstance(boc, bytes):
        raise TypeError("bytes can not be target buffer since readonly")
    if isinstance(boc, bytearray):
        # it's a bytearray
        return boc
    if not isinstance(boc, memoryview):
        try:
            boc = memoryview(boc)
        except TypeError:
            return None
    # it's a memoryview now
    if boc.readonly:
        raise TypeError("readonly memoryview can not be target buffer")
    if boc.nbytes == 0:  # if zero-length, replace with empty bytes
        # coz when a zero length ndarray is viewed, cast/send will raise while not needed at all
        return b""
    elif boc.itemsize != 1:
        return boc.cast("B")
    return boc
