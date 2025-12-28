from dataclasses import dataclass, field
from typing import Dict, Any, List, override, Optional

from ..record import IRecord


@dataclass
class EditionRecord(IRecord):
    """Class representing an edition record."""

    ol_id: str  # Open Library Identifier
    ocaid: str  # Internet Archive Identifier
    title: Optional[str]
    publishing_date_raw: Optional[str]
    publishing_date: int
    is_approximate: bool
    authors: List[str] = field(default_factory=list)  # List of author IDs
    languages: List[str] = field(default_factory=list)
    isbn_10: List[str] = field(default_factory=list)
    isbn_13: List[str] = field(default_factory=list)
    local_path: Optional[str] = None

    @override
    def as_dict(self) -> Dict[str, Any]:
        return {
            "ol_id": self.ol_id,
            "ocaid": self.ocaid,
            "title": self.title,
            "authors": self.authors,
            "publishing_date_raw": self.publishing_date_raw,
            "publishing_date": self.publishing_date,
            "is_approximate": self.is_approximate,
            "languages": self.languages,
            "isbn_10": self.isbn_10,
            "isbn_13": self.isbn_13,
            "local_path": self.local_path
        }

    @override
    def as_tuple(self) -> tuple[Any, ...]:
        return (
            self.ol_id,
            self.ocaid,
            self.title,
            self.authors,
            self.publishing_date_raw,
            self.publishing_date,
            self.is_approximate,
            self.languages,
            self.isbn_10,
            self.isbn_13,
            self.local_path
        )

    @override
    def __repr__(self):
        return f"EditionRecord(ol_id={self.ol_id}, ocaid={self.ocaid}, title={self.title}, path={self.local_path})"