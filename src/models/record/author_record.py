from dataclasses import dataclass, field
from typing import override, Optional

from .record_interface import IRecord


@dataclass
class AuthorRecord(IRecord):
    """Class representing an author record."""

    ol_id: str
    name: str
    death_date_raw: Optional[str] = field(default=None)
    death_date: int = field(default=-1)
    birth_date: int = field(default=-1)
    is_death_date_exact: bool = field(default=False)

    @override
    def __repr__(self):
        return f"AuthorRecord(ol_id={self.ol_id}, name={self.name}, death_date={self.death_date})"