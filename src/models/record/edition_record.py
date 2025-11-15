from dataclasses import dataclass, field
from typing import Dict, Any, List, override

from src.models.record.interface import IRecord


@dataclass
class EditionRecord(IRecord):
    """Class representing an edition record."""

    _ocaid: str  # Internet Archive Identifier
    title: str
    publishing_date: int
    copyright_date: int
    authors: List[str] = field(default_factory=list)  # List of author IDs
    languages: List[str] = field(default_factory=list)
    isbn_10: List[str] = field(default_factory=list)
    isbn_13: List[str] = field(default_factory=list)
    works: List[str] = field(default_factory=list)  # List of work IDs

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

