from typing import Any, override

from src.models.record import EditionRecord
from src.pipeline.steps import BaseAggregator


class CountingAggregator(BaseAggregator):
    def __init__(self):
        super().__init__()
        self.count = 0

    @override
    def update(self, data: Any) -> None:
        self.count += 1

    def get_count(self):
        return self.count


class LanguageCounterAggregator(BaseAggregator):
    def __init__(self):
        super().__init__()

    @override
    def update(self, data: EditionRecord) -> None:
        language = data.languages
        if language not in self.result:
            self.result[language] = 0
        self.result[language] += 1

    def get_language_counts(self):
        return self.result