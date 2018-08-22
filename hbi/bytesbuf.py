__all__ = [
    'BytesBuffer',
]


class BytesBuffer:
    """
    Wrap bytes/bytebuffer objects to make it possible for their data be consumed with nondeterministic paces
    """

    def __init__(self, bytes_, offset=0):
        if isinstance(bytes_, BytesBuffer):
            self.bytes_ = bytes_.bytes_
            self.offset = bytes_.offset + offset
        else:
            self.bytes_ = bytes_
            self.offset = offset

    def __len__(self):
        return len(self.bytes_) - self.offset

    def find(self, sub, start=None, end=None):
        ret = self.bytes_.find(sub, self.offset + (start or 0), (self.offset + end) if end else None)
        if ret > 0:
            return ret - self.offset
        return ret

    def consume(self, amount):
        self.offset += amount
        return self

    def data(self, start=0, end=None):
        return memoryview(self.bytes_)[self.offset + (start or 0): (self.offset + end) if end else None]
