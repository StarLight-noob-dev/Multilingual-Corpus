from dataclasses import dataclass, field
from typing import Dict, Any, override, Tuple

from src.models.record.interface import IRecord


@dataclass
class AuthorRecord(IRecord):
    """Class representing an author record."""
    name: str
    birth_date: int = field(default=-1)
    death_date: int = field(default=-1)
    is_death_date_exact: bool = field(default=False)
    _work_count: int = field(default=0)

    def add_work(self, amount: int = 1):
        self._work_count += amount


    def work(self):
        return self._work_count


    @override
    def as_dict(self) -> Dict[str, Any]:
        return {
            "ol_id": self.id,
            "name": self.name,
            "birth_date": self.birth_date,
            "death_date": self.death_date,
            "is_death_date_exact": self.is_death_date_exact,
            "work_count": self._work_count
        }


    @override
    def as_tuple(self) -> Tuple[Any, ...]:
        return (
            self.id,
            self.name,
            self.birth_date,
            self.death_date,
            self.is_death_date_exact,
            self._work_count
        )
