import gzip
import logging
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)


def unpack_gzip(file_path: str | Path, remove_original: bool = True) -> Path:
    """
    Unpack a .gz file to its base filename.

    Args:
        file_path (str | Path): The path to the .gz file.
        remove_original (bool): Whether to remove the original .gz file after unpacking. Defaults to True.

    Returns:
        The path to the unpacked file.
    """
    path = Path(file_path)
    # .with_suffix removes the last extension (file.txt.gz -> file.txt)
    unpacked_file_path = path.with_suffix('')

    logger.debug(f"Unpacking {file_path} to {unpacked_file_path}")

    try:
        with gzip.open(path, 'rb') as f_in:
            with open(unpacked_file_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        if remove_original:
            path.unlink()  # Remove the original .gz file after unpacking

        return unpacked_file_path
    except Exception as e:
        logger.error(f"Failed to unpack {file_path}: {e}")
        raise