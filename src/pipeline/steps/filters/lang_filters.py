from typing import List, override

from src.pipeline.steps import BaseFilter
from src.models.record import EditionRecord


class LanguageFilter(BaseFilter):
    """
    Filter to include or exclude data based on language.

    By default, it includes only the specified languages, if no languages are provided, it includes all languages.
    It can also be configured to exclude the specified languages.
    """

    def __init__(self, languages: List[str], include: bool = True):
        """
        Initialize the LanguageFilter.

        Args:
            languages (list): List of language codes to filter by.
            include (bool): If True, include only the specified languages; if False, exclude them.
        """
        self.languages = set(languages)
        self.include = include

    @override
    def filter(self, data: EditionRecord) -> bool:
        if self.include:
            return any(lang in self.languages for lang in data.languages)
        else:
            return all(lang not in self.languages for lang in data.languages)


class AnyLanguageFilter(BaseFilter):
    """
    Filter to include records that have at least one language specified.

    Args:
        dont_care (bool): If True, the filter will pass all records regardless if they contain a language.
    """
    def __init__(self, dont_care: bool = False):
        self.dont_care = dont_care

    @override
    def filter(self, data: EditionRecord) -> bool:
        return bool(data.languages) or self.dont_care
