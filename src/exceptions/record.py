from typing import Any

from .base_exception import PipelineError


class RecordError(PipelineError):
    """Base class for record-related exceptions."""

    def __init__(self, message: str, step_name: str = None, payload: Any = None):
        super().__init__(message, step_name, payload)


class UnknownRecordTypeError(RecordError):
    """Exception raised for unknown record types."""

    def __init__(self, record_type: str, step_name: str = None, payload: Any = None):
        self.record_type = record_type
        message = f"Unknown record type: {self.record_type}"
        super().__init__(message, step_name=step_name, payload=payload)


class RecordConversionError(RecordError):
    """Exception raised for errors during record conversion."""

    def __init__(self, message: str, step_name: str = None, payload: Any = None):
        super().__init__(message, step_name=step_name, payload=payload)
