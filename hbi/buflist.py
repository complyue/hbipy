__all__ = [
    'BufferList',
]


class BufferList:
    def __init__(self):
        self.head = None
        self.tail = None
        self.nbytes = 0

    def append(self, buf):
        if not buf or len(buf) <= 0:
            return
        entry = [buf, None]
        if self.nbytes == 0:
            self.head = entry
            self.tail = entry
        else:
            self.tail[1] = entry
            self.tail = entry
        self.nbytes += len(buf)

    def appendleft(self, buf):
        if not buf or len(buf) <= 0:
            return
        entry = [buf, self.head]
        if self.nbytes <= 0:
            self.tail = entry
        self.head = entry
        self.nbytes += len(buf)

    def popleft(self):
        if self.nbytes <= 0:
            return None
        ret = self.head[0]
        if self.head is self.tail:
            self.head = None
            self.tail = None
            self.nbytes = 0
        else:
            self.head = self.head[1]
            self.nbytes -= len(ret)
        return ret

    def clear(self):
        self.head = None
        self.tail = None
        self.nbytes = 0
