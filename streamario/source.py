class Source:
    def __init__(self, streamable):
        self.streamable = streamable

    def get(self, size=-1):
        return self.streamable.read(size)
