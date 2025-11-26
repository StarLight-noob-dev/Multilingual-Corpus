from abc import ABC
from dataclasses import dataclass, replace, field
from typing import Optional, Set


@dataclass(frozen=True)
class PipelineContext(ABC):
    run_id: str
    env: Optional[str] = None

    def with_updates(self, **kwargs):
        return replace(self, **kwargs)


@dataclass(frozen=True)
class IOFlags:
    """Generic I/O options that many stages may need.

    - write_shutdown_info: whether stages should append shutdown summaries
      to a file when asked.
    - shutdown_file_path: sensible default or override for the shutdown write.
    - append_mode: whether writes should append (keeps existing behaviour).
    - encoding: file encoding for text output.
    """

    write_shutdown_info: bool = False
    shutdown_file_path: Optional[str] = None
    append_mode: bool = True
    encoding: str = "utf-8"


@dataclass(frozen=True)
class LanguageFlags:
    languages: Optional[Set[str]] = None
    any_language: bool = False
    _default_languages: Set[str] = field(default_factory=lambda: {
        "en", "de", "fr", "it", "es", "hi", "ar", "ja", "zh", "ru"
    }, init=False)

    def get_effective_languages(self) -> Set[str]:
        if self.any_language:
            return set()
        if self.languages is not None:
            return self.languages
        return self._default_languages


@dataclass(frozen=True)
class LanguageContext(PipelineContext):
    flags: LanguageFlags = LanguageFlags()
    io: IOFlags = IOFlags()
