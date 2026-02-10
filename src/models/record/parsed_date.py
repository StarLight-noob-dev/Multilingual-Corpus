from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ParsedDate:
    """
    Represents a parsed date with its original string, the parsed year, and whether it's an exact date.
    """
    raw: Optional[str]
    parsed_val: int
    is_exact: bool
    reason: Optional[str] = None

    @property
    def is_known(self) -> bool:
        """Indicates whether the parsed date is known (i.e., has a valid parsed value)."""
        return self.parsed_val != -1

    @staticmethod
    def from_mapped_dict(d: dict) -> 'ParsedDate':
        """Creates a ParsedDate instance from a dictionary."""
        return ParsedDate(
            raw=d.get("raw"),
            parsed_val=d.get("parsed_val", -1),
            is_exact=d.get("is_exact", False),
            reason=d.get("reason")
        )