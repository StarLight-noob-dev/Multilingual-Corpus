from typing import List, Dict, Any, TypeVar, Generic

from src.models.results.types import Ok, Err

T = TypeVar("T")
E = TypeVar("E")


class StageResult(Generic[T, E]):

    def __init__(self, stage_name: str, details: str, *, has_failed: bool = False):
        """Create a StageResult that collects Ok[T] successes and Err[E] failures.

        Inputs/outputs (contract):

        - inputs: stage_name (str), details (str), optional has_failed flag
        - outputs: collects Ok[T] in `success` and Err[E] in `failed`
        - error modes: None raised by this class; it only stores values
        - success criteria: collected items accessible via typed helpers
        """
        self.stage_name = stage_name
        self.details = details
        # use a private attribute to avoid shadowing a method/property
        self._has_failed = has_failed
        self.success: List[Ok[T]] = []
        self.failed: List[Err[E]] = []

    def add_ok(self, record: Ok[T]) -> None:
        """Add a successful Ok[T] result."""
        self.success.append(record)

    def add_err(self, error: Err[E]) -> None:
        """Add an Err[E] failure result."""
        self.failed.append(error)

    def success_values(self) -> List[T]:
        """Returns the unwrapped values from Ok[T] records."""
        return [r.ok_value() for r in self.success]

    def failed_values(self) -> List[E]:
        """Returns the unwrapped error values from Err[E] records."""
        return [r.err_value() for r in self.failed]

    def has_success(self) -> bool:
        """Indicates if the stage successfully processed any records."""
        return len(self.success) > 0

    def has_errors(self) -> bool:
        """Indicates if the stage encountered any record-level errors during processing."""
        return len(self.failed) > 0

    @property
    def has_failed(self) -> bool:
        """Indicates if the stage encountered a catastrophic failure beyond record-level errors."""
        return self._has_failed

    @has_failed.setter
    def has_failed(self, value: bool) -> None:
        self._has_failed = bool(value)

    def mark_failed(self, value: bool = True) -> None:
        """Convenience to set the failed flag."""
        self._has_failed = bool(value)

    def summary(self) -> Dict[str, Any]:
        return {
            "stage_name": self.stage_name,
            "has_failed": self._has_failed,
            "total_processed": len(self.success) + len(self.failed),
            "total_success": len(self.success),
            "total_failed": len(self.failed),
            "details": self.details,
        }

    def __repr__(self) -> str:
        return (
            f"StageResult(stage_name={self.stage_name}, total_processed={len(self.success) + len(self.failed)}, "
            f"total_success={len(self.success)}, total_failed={len(self.failed)})"
        )

    def __len__(self) -> int:
        return len(self.success) + len(self.failed)
