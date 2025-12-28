class InvalidChunkBoundaryError(ValueError):
    """Exception raised for invalid chunk boundaries in data processing."""
    def __init__(self, start: int, end: int):
        self.message = f"Invalid chunk boundary encountered for [{start}-{end}] ."
        super().__init__(self.message)