from enum import Enum


class RecordStatus(int, Enum):
    """
    Available statuses for records that are being processed.
    Any record starts as PENDING, then moves to COMPLETED when done,
    or to ERROR if something went wrong. OMIT can be used to mark records
    that should be skipped.
    """
    PENDING = 0
    METADATA_EXTRACTED = 1 # First pass from current OL dumps, including download of books when possible.
    RAG_COMPLETED = 2      # RAG completed; Obtained snippet answers for the record.
    CONSENSUS_REACHED = 3  # ACL.2025 Median year calculation done.
    COMPLETED = 4
    ERROR = -1
