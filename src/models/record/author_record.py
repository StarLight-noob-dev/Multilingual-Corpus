from dataclasses import dataclass, field
from typing import Dict, Any, override, Tuple

from sqlalchemy import String, Integer, Boolean
from sqlalchemy.orm import Mapped, mapped_column

from src.database.base import Base
from src.models.record.interface import IRecord


@dataclass
class AuthorRecord(Base, IRecord):
    """Class representing an author record."""

    __tablename__ = "authors"

    _ol_id: Mapped[str] = mapped_column("id", primary_key=True) # Open Library Identifier

    name: Mapped[str] = mapped_column("name", String, nullable=False)
    birth_date: Mapped[int] = mapped_column("birth_date", Integer, nullable=False, default=-1)
    death_date: Mapped[int] = mapped_column("death_date", Integer, nullable=False, default=-1)
    is_death_date_exact: Mapped[bool] = mapped_column("death_exact", Boolean, default=False)
    _work_count: Mapped[int] = mapped_column("work_count", Integer, default=0)

    def add_work(self, amount: int = 1):
        self._work_count += amount


    def work(self):
        return self._work_count


    @override
    def as_dict(self) -> Dict[str, Any]:
        return {
            "ol_id": self.id,
            "name": self.name,
            "death_date": self.death_date,
            "is_death_date_exact": self.is_death_date_exact,
            "work_count": self._work_count
        }


    @override
    def as_tuple(self) -> Tuple[Any, ...]:
        return (
            self.id,
            self.name,
            self.death_date,
            self.is_death_date_exact,
            self._work_count
        )


    @override
    def __str__(self):
        return f"{self.id} {self.name} {self.death_date} {self.is_death_date_exact}"