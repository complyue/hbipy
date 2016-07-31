class WireError(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class UsageError(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
