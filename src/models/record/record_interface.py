import json
from abc import ABC
from dataclasses import asdict, astuple
from typing import Dict, Any, Tuple


class IRecord(ABC):
    """Interface for record types."""

    def to_json(self) -> str:
        """Convert the record to a JSON string representation."""
        return json.dumps(self.to_dict())

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the record to a dictionary representation.

        Returns:
            Dict[str, Any]: A dictionary representation of the record.
        """
        return asdict(self)

    def to_tuple(self) -> Tuple[Any, ...]:
        """
        Convert the record to a tuple representation.

        Returns:
            Tuple[Any, ...]: A tuple representation of the record.
        """
        return astuple(self)