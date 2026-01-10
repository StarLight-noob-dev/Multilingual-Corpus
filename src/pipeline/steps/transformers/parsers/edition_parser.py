import json
from typing import Optional, override

from src.common.year_parsing import extract_year
from src.exceptions.record import RecordConversionError
from src.models.record import TransportRecord, EditionRecord
from .record_parser import RecordParser


class EditionRecordParser(RecordParser):
    """Transformer to parse EditionRecord from TransportRecord"""
    @override
    def transform(self, t_record: TransportRecord) -> Optional[EditionRecord]:
        if t_record is None:
            return None

        if t_record.get_type() != "edition":
            raise RecordConversionError(f"Expected 'edition', got '{t_record.get_type()}'")

        try:
            data = json.loads(t_record.json_string)
        except json.JSONDecodeError as e:
            raise RecordConversionError(
                f"Failed to decode JSON for record {t_record.get_ol_id()}: {e.msg}",
                step_name=self.__class__.__name__,
                payload={"raw_json": t_record.json_string, "line": e.lineno}
            )

        ol_id = t_record.get_ol_id()
        ocaid = data.get("ocaid") or ""
        title = data.get("title") or ""
        publishing_date_raw = data.get("publish_date") or ""
        publishing_date, is_approximate = extract_year(publishing_date_raw, no_aprox=True)

        # Normalize authors: accept list of dicts or list of dicts {'key': '/authors/OL1A'}
        raw_authors = self._get_list(data, "authors")
        authors = self._extract_key_suffix(raw_authors)

        # Normalize languages: It's a list of dicts {'key': '/languages/eng'}
        raw_langs = self._get_list(data, "languages")
        languages = self._extract_key_suffix(raw_langs)

        # ISBNs - ensure lists of strings
        isbn_10 = [s for s in self._get_list(data, "isbn_10") if isinstance(s, str)]
        isbn_13 = [s for s in self._get_list(data, "isbn_13") if isinstance(s, str)]

        return EditionRecord(
            ol_id=ol_id,
            ocaid=ocaid,
            title=title,
            publishing_date_raw=publishing_date_raw,
            publishing_date=publishing_date,
            is_approximate=is_approximate,
            authors=authors,
            languages=languages,
            isbn_10=isbn_10,
            isbn_13=isbn_13,
            local_path=None
        )
