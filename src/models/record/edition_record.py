from dataclasses import dataclass, field
from typing import List, override, Optional, Dict

from .parsed_date import ParsedDate
from .record_interface import IRecord
from .record_status import RecordStatus


@dataclass
class EditionRecord(IRecord):
    """Class representing an edition record."""

    ol_id: str  # Open Library Identifier
    ocaid: str  # Internet Archive Identifier
    title: str
    publishing_date: ParsedDate = field(default_factory=lambda: ParsedDate(None, -1, False))
    authors: List[str] = field(default_factory=list)  # List of author IDs
    languages: List[str] = field(default_factory=list)
    isbn_10: List[str] = field(default_factory=list)
    isbn_13: List[str] = field(default_factory=list)
    local_path: Optional[str] = None
    status: RecordStatus = RecordStatus.PENDING
    error: Optional[str] = None
    retries: int = 0
    has_copyright: bool = False
    copyright_reason: Optional[str] = None

    # --- ACL.2025 fields ---
    temporal_estimates: Dict[str, int] = field(default_factory=dict)
    median_year: Optional[int] = None
    confidence_score: float = 0.0 # Agreement between estimates.
    is_refined_subset: bool = False

    #Number of characters, tokens and sentences as well as other structural statistics of the text.
    structural_statistics: Dict[str, int] = field(default_factory=dict)

    @override
    def __repr__(self):
        return f"EditionRecord(ol_id={self.ol_id}, ocaid={self.ocaid}, title={self.title}, path={self.local_path})"