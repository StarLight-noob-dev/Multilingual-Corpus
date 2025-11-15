import atexit
import logging
import os
from pathlib import Path
from threading import Lock
from typing import Union


class BufferedFileHandler(logging.Handler):
    """
    Buffer log records and write them to `filename` every `capacity` records
    or at program exit. Thread-safe.
    """
    def __init__(self, filename: Union[str, Path], capacity: int = 100, encoding: str | None = "utf-8"):
        super().__init__()
        self.filename = str(filename)
        self.capacity = int(capacity)
        self.encoding = encoding
        self._buffer: list[str] = []
        self._lock = Lock()
        # Use a default formatter if user doesn't set one
        self.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s'))

        # Ensure directory exists and touch file
        dirpath = os.path.dirname(self.filename) or "."
        os.makedirs(dirpath, exist_ok=True)
        if not os.path.exists(self.filename):
            # create file
            open(self.filename, 'a', encoding=self.encoding).close()

        # Ensure flush on exit
        atexit.register(self.flush)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            with self._lock:
                self._buffer.append(msg)
                if len(self._buffer) >= self.capacity:
                    self._write_buffer()
        except Exception:
            # follow logging.Handler contract: handle errors internally
            self.handleError(record)

    def _write_buffer(self) -> None:
        """Write buffer to disk (assumes lock held)."""
        if not self._buffer:
            return
        # ensure directory still exists
        dirpath = os.path.dirname(self.filename) or "."
        os.makedirs(dirpath, exist_ok=True)
        # write all buffered lines at once
        with open(self.filename, 'a', encoding=self.encoding) as f:
            f.write("\n".join(self._buffer) + "\n")
        self._buffer.clear()

    def flush(self) -> None:
        with self._lock:
            try:
                self._write_buffer()
            except Exception:
                # avoid raising in flush; log the exception safely
                logging.getLogger(__name__).exception("BufferedFileHandler.flush failed")

    def close(self) -> None:
        try:
            self.flush()
        finally:
            super().close()