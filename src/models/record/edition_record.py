from dataclasses import dataclass
from typing import Dict, Any, List, override

from sqlalchemy import String, Integer, JSON
from sqlalchemy.orm import Mapped, mapped_column

from src.database.base import Base
from src.models.record.interface import IRecord


@dataclass
class EditionRecord(Base, IRecord):
    """Class representing an edition record."""
    __tablename__ = "editions"

    _ol_id: Mapped[str] = mapped_column("id", primary_key=True)  # Open Library Identifier
    _ocaid: Mapped[str] = mapped_column("ocaid", String, nullable=False)  # Internet Archive Identifier
    title: Mapped[str] = mapped_column("title", String, nullable=False)

    publishing_date: Mapped[int] = mapped_column("publishing_date", Integer, default=-1)
    copyright_date: Mapped[int] = mapped_column("copyright_date", Integer, default=-1)

    authors: Mapped[List[str]] = mapped_column("authors", JSON, default=list)
    languages: Mapped[List[str]] = mapped_column("languages", JSON, default=list)
    isbn_10: Mapped[List[str]] = mapped_column("isbn_10", JSON, default=list)
    isbn_13: Mapped[List[str]] = mapped_column("isbn_13", JSON, default=list)
    works: Mapped[List[str]] = mapped_column("works", JSON, default=list)

    @property
    def ocaid(self) -> str:
        """Get the Internet Archive Identifier of the edition."""
        return self._ocaid

    @override
    def as_dict(self) -> Dict[str, Any]:
        return {
            "ol_id": self.id,
            "ocaid": self.ocaid,
            "title": self.title,
            "authors": self.authors,
            "publishing_date": self.publishing_date,
            "copyright_date": self.copyright_date,
            "languages": self.languages,
            "isbn_10": self.isbn_10,
            "isbn_13": self.isbn_13,
            "works": self.works,
        }


    @override
    def as_tuple(self) -> tuple[Any, ...]:
        return (
            self.id,
            self.ocaid,
            self.title,
            self.authors,
            self.publishing_date,
            self.copyright_date,
            self.languages,
            self.isbn_10,
            self.isbn_13,
            self.works,
        )

