from dataclasses import dataclass
from typing import Dict, Any, override, Tuple

from src.models.record.interface import IRecord


@dataclass
class TransportRecord(IRecord):
    """Class representing a transport record for inter-process communication."""

    json_string: str # JSON string representation of the record
    r_type: str

    @override
    def as_dict(self) -> Dict[str, Any]:
        return {
            "ol_id": self.id,
            "json_string": self.json_string
        }


    @override
    def as_tuple(self) -> Tuple[Any, ...]:
        return (self.id, self.json_string)