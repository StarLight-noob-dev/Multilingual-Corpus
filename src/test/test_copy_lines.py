from pathlib import Path

from utility.copy_lines import copy_lines


def write_file(path: Path, content: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def read_file(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_copy_basic(tmp_path):
    src = tmp_path / "src.txt"
    dst = tmp_path / "dst.txt"
    content = "line1\nline2\nline3\n"
    write_file(src, content)

    n = copy_lines(src, dst, max_lines=2)
    assert n == 2
    assert read_file(dst) == "line1\nline2\n"


def test_copy_with_start(tmp_path):
    src = tmp_path / "src.txt"
    dst = tmp_path / "dst.txt"
    content = "a\nb\nc\nd\n"
    write_file(src, content)

    n = copy_lines(src, dst, max_lines=2, start=1)
    assert n == 2
    assert read_file(dst) == "b\nc\n"


def test_copy_append(tmp_path):
    src = tmp_path / "src.txt"
    dst = tmp_path / "dst.txt"
    content = "x\ny\n"
    write_file(src, content)
    write_file(dst, "existing\n")

    n = copy_lines(src, dst, max_lines=10, append=True)
    assert n == 2
    assert read_file(dst) == "existing\nx\ny\n"


def test_copy_zero_max(tmp_path):
    src = tmp_path / "src.txt"
    dst = tmp_path / "dst.txt"
    write_file(src, "one\ntwo\n")

    n = copy_lines(src, dst, max_lines=0)
    assert n == 0
    assert read_file(dst) == ""


def test_start_past_eof(tmp_path):
    src = tmp_path / "src.txt"
    dst = tmp_path / "dst.txt"
    write_file(src, "only\n")

    n = copy_lines(src, dst, max_lines=5, start=10)
    assert n == 0
    assert read_file(dst) == ""


def test_preserve_unix_newlines(tmp_path):
    # Source may have mixed line endings; output should use "\n"
    src = tmp_path / "src.txt"
    dst = tmp_path / "dst.txt"
    # include CRLF and LF
    content = "r1\r\n r2\n r3\r\n"
    write_file(src, content)

    n = copy_lines(src, dst, max_lines=3)
    assert n == 3
    assert read_file(dst) == "r1\n r2\n r3\n"

