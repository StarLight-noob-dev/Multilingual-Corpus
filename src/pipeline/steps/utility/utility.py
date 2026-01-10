import threading
from typing import Any, override

from src.pipeline.steps import PipelineStep


class EarlyPipelineStep(PipelineStep):
    def __init__(self, limit: int):
        self.limit = limit
        self._counter = 0
        self._lock = threading.Lock()

    @override
    def execute(self, data: Any) -> Any:
        with self._lock:
            self._counter += 1
            if self._counter > self.limit:
                from src.exceptions import TestingLimitReached
                raise TestingLimitReached(
                    step_name="EarlyPipelineStep",
                    limit=self.limit
                )
        return data