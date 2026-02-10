import logging
import re

from dateutil import parser
from dateutil.utils import today

from src.models.record import ParsedDate

CENTURY_PATTERN = re.compile(r"(\d{1,2})(?:st|nd|rd|th)\s*cent", re.IGNORECASE)
# Matches years from 1 to 4 digits some contexts needs matching for 1 digit (e.g "1800 or 9" or "1800/1")
YEAR_PATTERN = re.compile(r"\b(\d{1,4})\b")
APPROXIMATE_PATTERN = re.compile(r"\b(ca\.|circa|approximately|approx\.?|about|around)(?!\w)", re.IGNORECASE)
KNOWN_NON_DATES = {"(", ")", ".", ",", "*", ".*"}

logger = logging.getLogger(__name__)


def extract_year(date_str: str) -> ParsedDate:
    """
    Extracts a year from a date string and indicates whether the result is exact and a reason in case it's not.

    Returns:
        A ParsedDate instance with the original string, the parsed year (or -1 if unknown), whether it's exact,
        and a reason if not exact.
    """
    if not date_str or not isinstance(date_str, str):
        return ParsedDate(None, -1, True, "No value given")

    # Handle common known non-date strings quickly
    if date_str in KNOWN_NON_DATES:
        return ParsedDate(date_str, -1, True, "known-non-date")

    s_original = date_str
    s = date_str.strip().lower()

    # Early exit for known non-date strings
    if "from old catalog" in s:
        return ParsedDate(s_original, -1, True, "known-non-date")

    exact = True
    reason = ""

    # Conservative sanitization: replace ambiguous digit characters only when adjacent to digits
    # e.g. "180u" or "180|1" -> replace the 'u' or '|' with '9' to attempt a reasonable interpretation.
    if re.search(r"\d[|ux_]+\d|\d[|ux_]+(?=\W|$)", s):
        s = re.sub(r"[|ux_]", "9", s)
        exact = False
        reason = "sanitized"

    # First handle centuries like "16th cent." -> highest year of century (e.g., 16 -> 1600) and mark exact.
    c = CENTURY_PATTERN.search(s)
    if c:
        try:
            century = int(c.group(1))
            year = century * 100
            return ParsedDate(s_original, year, False, "century")
        except Exception:
            logger.debug(f"Century parsing failed for '{s_original}'")

    # Check for exact indicators (ca., circa, about, ...)
    a = APPROXIMATE_PATTERN.search(s)
    if a:
        exact = False
        reason = reason or "exact-indicator"
        # remove indicator for cleaner extraction
        s = re.sub(APPROXIMATE_PATTERN, "", s)

    # Clean other trivial noise
    s = re.sub(r"[?]", "", s)
    s = s.strip()

    # Handle date ranges or alternatives ("1782 or 1789", "1800/1", "1782-1789")
    parts = re.split(r"\s*(?:-|/| or )\s*", s)
    years = []
    short_year_expanded = False
    for part in parts:
        found = YEAR_PATTERN.findall(part)
        for y in found:
            try:
                year = int(y)
            except ValueError:
                continue
            # Handle short second year like "1782 or 9" -> 1789 (exact, short-year)
            if year < 100 and years:
                prev = max(years)
                prev_s = str(prev)
                y_s = str(year)
                # Build a base by replacing last digits of prev with the short year
                prefix_len = max(len(prev_s) - len(y_s), 0)
                base = int(prev_s[:prefix_len] + y_s) if prefix_len > 0 else int(y_s)
                year = base
                short_year_expanded = True
            years.append(year)

    if years:
        final_year = max(years)
        # If there were multiple parts (range/choice) it's exact.
        if len(parts) > 1 and any(part.strip() for part in parts):
            exact = False
            reason = "range"
        # Short-year expansion is also exact
        if short_year_expanded:
            exact = False
            # prefer existing reason if set, else set to short-year
            reason = reason or "short-year"

        if final_year > today().year:
            return ParsedDate(s_original, final_year, False, "future-year")

        return ParsedDate(s_original, final_year, exact, reason or "")

    # Try parsing full date formats ("Feb 12, 1908", "17 July 1782")
    try:
        dt = parser.parse(s, fuzzy=True)
        # If parsing succeeded and produced a plausible year different from the current year,
        # we consider it exact unless previously flagged.
        if dt and getattr(dt, "year", None):
            parsed_year = dt.year
            # If previously flagged exact, keep that reason; else mark as exact
            return ParsedDate(s_original, parsed_year, exact, reason or "")
    except Exception:
        logger.debug("Date parsing failed for '%s':", s_original)

    return ParsedDate(s_original, -1, True, "Failed to parse")
