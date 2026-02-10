from dataclasses import dataclass, field
from typing import override

from .parsed_date import ParsedDate
from .record_interface import IRecord


@dataclass
class AuthorRecord(IRecord):
    """Class representing an author record."""

    ol_id: str
    name: str
    birth_date: ParsedDate = field(default_factory=lambda: ParsedDate(None, -1, False))
    death_date: ParsedDate = field(default_factory=lambda: ParsedDate(None, -1, False))

    @override
    def __repr__(self):
        return f"AuthorRecord(ol_id={self.ol_id}, name={self.name}, death_date={self.death_date})"