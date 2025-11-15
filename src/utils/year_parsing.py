import logging
import re
from typing import Tuple

from dateutil import parser

CENTURY_PATTERN = re.compile(r"(\d{1,2})(?:st|nd|rd|th)\s*cent", re.IGNORECASE)
# Matches years from 1 to 4 digits some contexts needs matching for 1 digit (e.g "1800 or 9" or "1800/1")
YEAR_PATTERN = re.compile(r"\b(\d{1,4})\b")
APPROXIMATE_PATTERN = re.compile(r"\b(ca\.|circa|approximately|approx\.?|about|around)(?!\w)", re.IGNORECASE)
KNOWN_NON_DATES = {"(", ")", ".", ",", "*", ".*"}

logger = logging.getLogger(__name__)


def extract_year(date_str: str, no_aprox: bool = False, *, adjustment: int = 5) -> Tuple[int, bool]:
    """
    Extracts the most specific year from a given date string. If the string is not clear it will approximate the year
    to the highest possible year found matching the descriptions.

    Examples of date strings it can handle:

    - "1782" -> 1782
    - "ca. 1782" -> 1782
    - "16th cent." -> 1600
    - "Feb 12, 1908" -> 1908
    - "1782 or 1789" -> 1789
    - "1800/1" -> 1801

    If no valid year can be extracted, it returns -1.

    The approximate flag is `True` if the year is derived from an approximate date (e.g., "ca. 1782").

    For certain formats that contains an approximate indicator, the year will be adjusted by adding a fixed number
    of years (default is 5) to account for uncertainty, unless the `no_aprox` parameter flag is set to True.
    (e.g., "ca. 1782" -> 1787)

    Note: This function prioritizes extracting the highest possible year from the string and its thought for gathering
    the **death year** of authors from uncertain date strings.

    Args:
        date_str (str): The date string to parse.
        no_aprox (bool): If True, for cases where an approximate indicator is found, the year will be returned
        without any approximation adjustment.
        adjustment (int): The number of years to add for approximate dates. Default is 5.

    Returns:
        A tuple containing a year (int) and a boolean indicating if the year is approximate.
        If no valid year is found, returns (-1, False).
    """
    if not date_str or not isinstance(date_str, str):
        return -1, False

    # Handle common known non-date strings quickly
    if date_str in KNOWN_NON_DATES:
        return -1, False

    s = date_str.strip().lower()

    # Early exit for known non-date strings
    if "from old catalog" in s:
        return -1, False

    # First Handle centuries like "16th cent."
    c = CENTURY_PATTERN.search(s)
    if c:
        century = int(c.group(1))
        # 16th century = 1501–1600 -> highest = 1600
        return century * 100, True

    # Check for approximate indicators
    a = APPROXIMATE_PATTERN.search(s)

    # Clean known prefixes / uncertainty indicators before year extraction for easier parsing.
    # If no approximation indicator is found, we still clean them for better year extraction later.
    s = re.sub(APPROXIMATE_PATTERN, "", s)
    s = re.sub(r"[?]", "", s)
    s = s.strip()

    if a:
        # If there is one approximate indicator, we treat the year as approximate do + `adjustment` years to be safe.
        f = YEAR_PATTERN.findall(s)
        if f:
            year = max(int(y) for y in f)
            if no_aprox:
                return year, False
            return year + adjustment, True

    # Handle date ranges or alternatives ("1782 or 1789", "1800/1")
    parts = re.split(r"[-/]| or ", s)
    years = []
    for part in parts:
        found = YEAR_PATTERN.findall(part)
        for y in found:
            year = int(y)
            # Handle short second year like "1782 or 9" -> 1789
            if year < 100 and years:
                prev = max(years)
                base = int(str(prev)[:len(str(prev)) - len(str(year))] + str(year))
                year = base
            years.append(year)
    if years:
        return max(years), True if len(parts) > 1 else False

    # Try parsing full date formats ("Feb 12, 1908", "17 July 1782")
    try:
        dt = parser.parse(s, fuzzy=True, default=None)
        if dt.year:
            return dt.year, False
    except Exception:
        logger.debug("Date parsing failed for '%s':", date_str)

    return -1, False

__all__ = [
    KNOWN_NON_DATES,
    "extract_year"
]