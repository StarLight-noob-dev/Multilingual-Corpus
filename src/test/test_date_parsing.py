from src.common.year_parsing import extract_year
from src.models.record import ParsedDate


class TestDateParsing:
    def test_simple_exact_year(self):
        x = extract_year("2000")
        assert x.parsed_val == 2000
        assert x.is_exact is True

    def test_approximate_indicator(self):
        x = extract_year("ca. 1995")
        assert x.parsed_val == 1995
        assert x.is_exact is False

    def test_century_parsing(self):
        x = extract_year("18th cent.")
        assert x.parsed_val == 1800
        assert x.is_exact is False

    def test_date_range(self):
        x = extract_year("1990/1")
        assert x.parsed_val == 1991
        assert x.is_exact is False

    def test_date_range_short(self):
        x = extract_year("1990 or 9")
        assert x.parsed_val == 1999
        assert x.is_exact is False

    def test_multiple_years(self):
        x = extract_year("1980 or 1985")
        assert x.parsed_val == 1985
        assert x.is_exact is False
        x = extract_year("1990 or 1800")
        assert x.parsed_val == 1990
        assert x.is_exact is False

    def test_no_valid_year(self):
        assert extract_year("from old catalog") == ParsedDate("from old catalog", -1, True, "known-non-date")
        assert extract_year("unknown date") == ParsedDate("unknown date", -1, True, "Failed to parse")
        assert extract_year("") == ParsedDate(None, -1, True, "No value given")
        assert extract_year(None) == ParsedDate(None, -1, True, "No value given")

    def test_fully_written_date(self):
        x = extract_year("March 3, 1876")
        assert x.parsed_val == 1876
        assert x.is_exact is True

    def test_sanitized_year_1(self):
        x = extract_year("180u")
        assert x.parsed_val == 1809
        assert x.is_exact is False
        assert x.reason == "sanitized"

    def test_sanitized_year_2(self):
        x = extract_year("18||")
        assert x.parsed_val == 1899
        assert x.is_exact is False
        assert x.reason == "sanitized"

    def test_sanitized_year_3(self):
        x = extract_year("18_1")
        assert x.parsed_val == 1891
        assert x.is_exact is False
        assert x.reason == "sanitized"