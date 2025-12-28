from dataclasses import dataclass, field
from typing import Dict, Any, override, Tuple, Optional

from ..record import IRecord


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
    def as_dict(self) -> Dict[str, Any]:
        return {
            "ol_id": self.ol_id,
            "name": self.name,
            "death_date_raw": self.death_date_raw,
            "death_date": self.death_date,
            "is_death_date_exact": self.is_death_date_exact,
        }

    @override
    def as_tuple(self) -> Tuple[Any, ...]:
        return (
            self.ol_id,
            self.name,
            self.death_date_raw,
            self.death_date,
            self.is_death_date_exact,
        )

    @override
    def __repr__(self):
        return f"AuthorRecord(ol_id={self.ol_id}, name={self.name}, death_date={self.death_date})"