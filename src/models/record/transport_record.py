from dataclasses import dataclass

from src.models.record import IRecord


@dataclass
class TransportRecord(IRecord):
    """Class representing a transport record for inter-process communication."""

    ol_id: str  # Open Library Identifier
    json_string: str # JSON string representation of the record
    r_type: str

    def get_ol_id(self) -> str:
        return self.ol_id.split('/')[-1]

    def get_type(self) -> str:
        return self.r_type.split('/')[-1]
