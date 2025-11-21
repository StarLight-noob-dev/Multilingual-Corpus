from src.utils.year_parsing import extract_year

class TestDateParsing:
    def test_simple_exact_year(self):
        year, approx = extract_year("2000")
        assert year == 2000
        assert approx is False

    def test_approximate_indicator(self):
        year, approx = extract_year("ca. 1995")
        assert year == 2000  # 1995 + 5
        assert approx is True

    def test_century_parsing(self):
        year, approx = extract_year("18th cent.")
        assert year == 1800
        assert approx is True

    def test_date_range(self):
        year, approx = extract_year("1990/1")
        assert year == 1991
        assert approx is True

    def test_multiple_years(self):
        year, approx = extract_year("1980 or 1985")
        assert year == 1985
        assert approx is True
        assert extract_year("1990 or 1800") == (1990, True)

    def test_no_valid_year(self):
        assert extract_year("from old catalog") == (-1, False)
        assert extract_year("unknown date") == (-1, False)
        assert extract_year("") == (-1, False)
        assert extract_year(None) == (-1, False)

    def test_fully_written_date(self):
        year, approx = extract_year("March 3, 1876")
        assert year == 1876
        assert approx is False

    def test_no_approx_flag(self):
        year, approx = extract_year("ca. 1800", no_aprox=True)
        assert year == 1800
        assert approx is False