import json
from typing import Optional

from src.models.record import TransportRecord, AuthorRecord
from src.exceptions.record import RecordConversionError
from src.common.year_parsing import extract_year
from .record_parser import RecordParser


class AuthorRecordParser(RecordParser):
    """Transformer to parse AuthorRecord from TransportRecord"""
    def transform(self, t_record: TransportRecord) -> Optional[AuthorRecord]:
        if t_record is None:
            return None

        if t_record.get_type() != "author":
            raise RecordConversionError(f"Expected 'author', got '{t_record.get_type()}'")

        try:
            data = json.loads(t_record.json_string)
        except json.JSONDecodeError as e:
            raise RecordConversionError(
                f"Failed to decode JSON for record {t_record.get_ol_id()}: {e.msg}",
                step_name=self.__class__.__name__,
                payload={"raw_json": t_record.json_string, "line": e.lineno}
            )

        ol_id = t_record.get_ol_id()
        name = self._get_str(data, "name")

        raw_birth_date = self._get_str(data, "birth_date")
        raw_death_date = self._get_str(data, "death_date")
        birth_date, _ = extract_year(raw_birth_date, no_aprox=True)  # We don't care about birthdate exactness
        death_date, exact = extract_year(raw_death_date, no_aprox=True)

        return AuthorRecord(
            ol_id=ol_id,
            name=name,
            death_date_raw=raw_death_date,
            death_date=death_date,
            birth_date=birth_date,
            is_death_date_exact=exact
        )