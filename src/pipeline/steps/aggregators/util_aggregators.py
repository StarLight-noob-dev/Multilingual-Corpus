from typing import Any, override, Optional

from src.models.record import EditionRecord
from src.pipeline.steps import ThreadBaseAggregator


class CountingAggregator(ThreadBaseAggregator):
    def __init__(self):
        super().__init__()
        self.count = 0

    @override
    def update(self, data: Any) -> None:
        with self._lock:
            self.count += 1

    def get_count(self):
        return self.count


class LanguageCounterAggregator(ThreadBaseAggregator):
    def __init__(self):
        super().__init__()

    @override
    def update(self, data: EditionRecord) -> None:
        languages = data.languages

        l = "NA"

        if len(languages) == 1:
            l = languages[0]
        elif len(languages) > 1:
            l = "multiple"
        with self._lock:
            if l not in self.result:
                self.result[l] = 0
            self.result[l] += 1

    def get_language_counts(self):
        return self.result