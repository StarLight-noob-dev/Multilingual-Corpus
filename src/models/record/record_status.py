from dataclasses import dataclass
from enum import Enum
from typing import Optional


class RecordStatus(str, Enum):
    """
    Available statuses for records that are being processed.
    Any record starts as PENDING, then moves to COMPLETED when done,
    or to ERROR if something went wrong. OMIT can be used to mark records
    that should be skipped.
    """
    PENDING = "PENDING"
    METADATA_EXTRACTED = "METADATA_EXTRACTED" # First pass from current OL dumps, including download of books when possible.
    RAG_COMPLETED = "RAG_COMPLETED"      # RAG completed; Obtained snippet answers for the record.
    CONSENSUS_REACHED = "CONSENSUS_REACHED"  # ACL.2025 Median year calculation done.
    COMPLETED = "COMPLETED"
    ERROR = "ERROR"
    OMIT = "OMIT"


class CopyrightStatus(str, Enum):
    """
    Available copyright statuses for records.
    UNKNOWN is the default value when the status is not known.
    PUBLIC_DOMAIN means the record is in the public domain and can be freely used.
    IN_COPYRIGHT means the record is still under copyright and cannot be freely used.
    """
    UNKNOWN = "UNKNOWN"
    PUBLIC_DOMAIN = "PUBLIC_DOMAIN"
    IN_COPYRIGHT = "IN_COPYRIGHT"


@dataclass(frozen=True)
class CopyrightInfo:
    """ Represents the copyright status of a record, along with an optional reason. Default status is UNKNOWN. """
    status: CopyrightStatus = CopyrightStatus.UNKNOWN
    reason: Optional[str] = None  # Reason for the copyright status, if known.

    @property
    def has_copyright(self) -> bool:
        """Returns True if the record is under copyright, False otherwise."""
        return self.status == CopyrightStatus.IN_COPYRIGHT

    def with_reason(self, reason: Optional[str]) -> 'CopyrightInfo':
        """Return a new CopyrightInfo with the same status and the supplied reason."""
        return CopyrightInfo(status=self.status, reason=reason)

    def to_dict(self) -> dict:
        return {
            "status": self.status.value,
            "reason": self.reason
        }

    @staticmethod
    def from_dict(d: dict) -> 'CopyrightInfo':
        status_raw = d.get("status")
        status = CopyrightStatus(status_raw) if status_raw is not None else CopyrightStatus.UNKNOWN
        return CopyrightInfo(status=status, reason=d.get("reason"))


@dataclass(frozen=True)
class StageInfo:
    """ Represents the status of a processing stage for a record, along with an optional message and timestamp.
    Default status is PENDING.
    """
    status: RecordStatus = RecordStatus.PENDING
    message: Optional[str] = None  # Reason for failure or extra metadata
    timestamp: Optional[str] = None # When this stage finished

    def with_message(self, message: Optional[str]) -> 'StageInfo':
        """Return a new StageInfo with the same status and the supplied message."""
        return StageInfo(status=self.status, message=message, timestamp=self.timestamp)

    def to_dict(self) -> dict:
        return {
            "status": self.status.value,
            "message": self.message,
            "timestamp": self.timestamp
        }

    @staticmethod
    def from_dict(d: dict) -> 'StageInfo':
        status_raw = d.get("status")
        status = RecordStatus(status_raw) if status_raw is not None else RecordStatus.PENDING
        return StageInfo(status=status, message=d.get("message"), timestamp=d.get("timestamp"))