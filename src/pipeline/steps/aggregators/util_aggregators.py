from typing import Any, override

from src.pipeline.steps import BaseAggregator


class CountingAggregator(BaseAggregator):
    def __init__(self):
        super().__init__()
        self.count = 0

    @override
    def update(self, data: Any) -> Any:
        self.count += 1

    def get_count(self):
        return self.count