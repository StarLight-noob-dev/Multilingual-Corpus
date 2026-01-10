import threading
from typing import Any, override

from src.pipeline.steps import PipelineStep


class EarlyPipelineStop(PipelineStep):
    def __init__(self, limit: int):
        """
        A pipeline step that raises an exception when a certain number of non-None data items have been processed.

        Args:
            limit (int): The maximum number of non-None data items to process before raising an exception.
        """
        self.limit = limit
        self._counter = 0
        self._lock = threading.Lock()

    @override
    def execute(self, data: Any) -> Any:
        if data is None:
            return data  # Only count non-None data
        with self._lock:
            self._counter += 1
            if self._counter > self.limit:
                from src.exceptions import TestingLimitReached
                raise TestingLimitReached(
                    step_name="EarlyPipelineStop",
                    limit=self.limit
                )
        return data