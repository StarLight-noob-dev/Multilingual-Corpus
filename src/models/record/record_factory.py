import json
import logging
from typing import Tuple

from src.exception.record import UnknownRecordTypeError
from src.models.record.author_record import AuthorRecord
from src.models.record.edition_record import EditionRecord
from src.models.record.interface import IRecord
from src.models.record.transport_record import TransportRecord
from src.models.record.work_record import WorkRecord
from src.utils.year_parsing import extract_year

logger = logging.getLogger(__name__)


def process_record(t_record: TransportRecord) -> IRecord:
    """
    Processes a TransportRecord and returns the corresponding model instance.

    Args:
        t_record (TransportRecord): The transport record to process.

    Raises:
        UnknownRecordTypeError: If the type of record does not match any known types.
    """
    if t_record is None:
        raise ValueError("TransportRecord cannot be None")

    record_type = t_record.r_type.split('/')[-1]

    match record_type:
        case "work":
            # Work processing not implemented yet
            raise NotImplementedError("work record processing not implemented")
        case "edition":
            return _process_edition_record(t_record)
        case "author":
            return _process_author_record(t_record)
        case _:
            raise UnknownRecordTypeError(record_type)


def _process_edition_record(t_record: TransportRecord) -> EditionRecord:
    data = json.loads(t_record.json_string)

    ol_id = t_record.id.split('/')[-1]
    ocaid = data.get("ocaid") or ""
    title = data.get("title")
    publishing_date, _ = extract_year(_get_str(data, "publish_date"), True)
    copyright_date, _ = extract_year(_get_str(data, "copyright_date", alt_fields=["copyright"]), True)

    # Normalize authors: accept list of dicts or list of dicts {'key': '/authors/OL1A'}
    raw_authors = _get_list(data, "authors")
    authors = []
    for a in raw_authors:
        if isinstance(a, dict):
            val = a.get("key")
            authors.append(val.split("/")[-1])

    # Normalize languages: It's a list of dicts {'key': '/languages/eng'}
    raw_langs = _get_list(data, "languages")
    languages = []
    for l in raw_langs:
        if isinstance(l, dict):
            val = l.get("key")
            languages.append(val.split("/")[-1])

    # ISBNs - ensure lists of strings
    isbn_10 = [s for s in _get_list(data, "isbn_10") if isinstance(s, str)]
    isbn_13 = [s for s in _get_list(data, "isbn_13") if isinstance(s, str)]

    # Works: same as authors - list of dicts {'key': '/works/OL1W'}
    raw_works = _get_list(data, "works")
    works = []
    for w in raw_works:
        if isinstance(w, dict):
            val = w.get("key")
            works.append(val.split("/")[-1])

    return EditionRecord(
        ol_id,
        ocaid,
        title,
        publishing_date,
        copyright_date,
        authors,
        languages,
        isbn_10,
        isbn_13,
        works
    )


def _process_author_record(t_record: TransportRecord) -> AuthorRecord:
    data = json.loads(t_record.json_string)

    ol_id = t_record.id.split('/')[-1]
    name = _get_str(data, "name")
    birth_date, _ = _parse_year(_get_str(data, "birth_date"))
    death_date, exact = _parse_year(_get_str(data, "death_date"))

    return AuthorRecord(
        ol_id,
        name,
        birth_date,
        death_date,
        exact
    )


def _process_work_record(t_record: TransportRecord) -> WorkRecord:
    pass


def _parse_year(date_str: str) -> Tuple[int, bool]:
    return extract_year(date_str)


def _get_str(data: dict, field: str, alt_fields=None) -> str:
    """
    Helper to get a string field content with optional alternative fields or fallback empty string
    """
    if alt_fields is None:
        alt_fields = []
    val = data.get(field)
    if isinstance(val, str):
        return val
    for af in alt_fields:
        v2 = data.get(af)
        if isinstance(v2, str):
            return v2
    return ""


def _get_list(data: dict, field: str) -> list:
    """
    Helper to get a list field, normalizing single values into a list
    """
    val = data.get(field)
    if val is None:
        return []
    if isinstance(val, list):
        return val
    return [val]

__all__ = ["process_record"]