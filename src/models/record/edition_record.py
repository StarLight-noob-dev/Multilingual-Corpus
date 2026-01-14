from dataclasses import dataclass, field
from typing import List, override, Optional

from .record_interface import IRecord
from .record_status import RecordStatus


@dataclass
class EditionRecord(IRecord):
    """Class representing an edition record."""

    ol_id: str  # Open Library Identifier
    ocaid: str  # Internet Archive Identifier
    title: str
    publishing_date: int
    is_approximate: bool
    publishing_date_raw: Optional[str] = None
    authors: List[str] = field(default_factory=list)  # List of author IDs
    languages: List[str] = field(default_factory=list)
    isbn_10: List[str] = field(default_factory=list)
    isbn_13: List[str] = field(default_factory=list)
    local_path: Optional[str] = None
    status: RecordStatus = RecordStatus.PENDING
    error: Optional[str] = None
    retries: int = 0

    @override
    def __repr__(self):
        return f"EditionRecord(ol_id={self.ol_id}, ocaid={self.ocaid}, title={self.title}, path={self.local_path})"