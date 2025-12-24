"""
Utility to copy up to N lines from one text file to another using "\n" as delimiter.

Functions:
- copy_lines(src_path, dst_path, max_lines, start=0, append=False)

CLI usage:
python -m utility.copy_lines --src source.txt --dst dest.txt --max 100 [--start 0] [--append]
"""
from pathlib import Path
import argparse
from typing import Union


def copy_lines(src_path: Union[str, Path], dst_path: Union[str, Path], max_lines: int, start: int = 0, append: bool = False) -> int:
    """
    Copy up to `max_lines` lines from `src_path` to `dst_path`.

    Lines are split by the newline character "\\\\n". This function streams the source file
    so it works with large files. It writes the copied lines to the destination file and
    returns the number of lines actually written.

    Args:
        src_path: path to source .txt file
        dst_path: path to destination .txt file
        max_lines: maximum number of lines to copy (must be >= 0)
        start: start copying from this zero-based line number in the source file
        append: if True, append to `dst_path`, otherwise overwrite

    Returns:
        The number of lines written to the destination file.
    """
    src = Path(src_path)
    dst = Path(dst_path)

    if max_lines < 0:
        raise ValueError("max_lines must be >= 0")
    if start < 0:
        raise ValueError("start must be >= 0")

    mode = "a" if append else "w"
    written = 0

    # Ensure parent directory exists
    if not dst.parent.exists():
        dst.parent.mkdir(parents=True, exist_ok=True)

    # Open source with default newline handling (None) so Python will normalize
    # any of '\r', '\n', or '\r\n' to '\n' on input. Open destination with
    # newline='\n' to ensure output lines use a single '\n' delimiter.
    with src.open("r", encoding="utf-8") as fin, dst.open(mode, encoding="utf-8", newline="\n") as fout:
        # Skip lines until start
        for _ in range(start):
            line = fin.readline()
            if line == "":
                # reached EOF before start
                return 0
        # Now copy up to max_lines lines
        while written < max_lines:
            line = fin.readline()
            if line == "":
                break
            # Strip any trailing newline characters and write with a single '\n'
            text = line.rstrip("\r\n")
            fout.write(text + "\n")
            written += 1

    return written


def _cli():
    parser = argparse.ArgumentParser(description="Copy up to N lines from one .txt file to another (delimiter: \"\\n\").")
    parser.add_argument("--src", required=True, help="Source .txt file path")
    parser.add_argument("--dst", required=True, help="Destination .txt file path")
    parser.add_argument("--max", required=True, type=int, help="Maximum number of lines to copy (>=0)")
    parser.add_argument("--start", type=int, default=0, help="Start copying at this zero-based line index (default 0)")
    parser.add_argument("--append", action="store_true", help="Append to destination instead of overwriting")
    args = parser.parse_args()
    try:
        count = copy_lines(args.src, args.dst, args.max, start=args.start, append=args.append)
        print(f"Wrote {count} lines from {args.src} to {args.dst}")
    except Exception as e:
        parser.error(str(e))


if __name__ == "__main__":
    _cli()
