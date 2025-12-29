from enum import Enum


class RecordStatus(str, Enum):
    """
    Available statuses for records that are being processed.
    Any record starts as PENDING, then moves to COMPLETED when done,
    or to ERROR if something went wrong. OMIT can be used to mark records
    that should be skipped.
    """
    PENDING = "pending"
    COMPLETED = "completed"
    OMIT = "OMIT"
    ERROR = "error"
