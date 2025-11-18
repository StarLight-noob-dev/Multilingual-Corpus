from src.exception.chunk import InvalidChunkBoundaryError


class Chunk:
    def __init__(self, file_name: str, start: int, end: int):
        self.file_name = file_name
        if start > end or start < 0 or end < 0 or start == end:
            raise InvalidChunkBoundaryError(start, end)
        self.start = start
        self.end = end

    def size(self):
        return self.end - self.start

    def __iter__(self):
        yield self.file_name
        yield self.start
        yield self.end

    def __str__(self):
        return f"Chunk of file {self.file_name} from {self.start} to {self.end}"