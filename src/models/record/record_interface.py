import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Any, Tuple


@dataclass
class IRecord(ABC):
    """Interface for record types."""

    _ol_id: str  # Open Library Identifier

    @property
    def id(self) -> str:
        """Get the Open Library Identifier of the record."""
        return self._ol_id

    def as_json(self) -> str:
        """Convert the record to a JSON string representation."""
        return json.dumps(self.as_dict())

    @abstractmethod
    def as_dict(self) -> Dict[str, Any]:
        """
        Convert the record to a dictionary representation.

        Returns:
            Dict[str, Any]: A dictionary representation of the record.
        """
        raise NotImplementedError()

    @abstractmethod
    def as_tuple(self) -> Tuple[Any, ...]:
        """
        Convert the record to a tuple representation.

        Returns:
            Tuple[Any, ...]: A tuple representation of the record.
        """
        raise NotImplementedError()