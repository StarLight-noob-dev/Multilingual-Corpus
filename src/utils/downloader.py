import os
import time
import gzip
import shutil
import logging
from typing import List, Optional
from internetarchive import get_item

logger = logging.getLogger()


class IADownloadManager:
    def __init__(self, base_dir: str = "downloads", delay: float = 2.0) -> None:
        self.base_dir = base_dir
        self.delay = delay
        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir)

    def download(self, identifier: str, formats: List[str] = None,
                 extensions: List[str] = None, unpack: bool = True) -> Optional[str]:
        """
        Download files from an Internet Archive item based on specified formats or extensions for a given identifier.

        Args:
            identifier (str): The Internet Archive item identifier.
            formats (List[str]): List of IA formats to filter files (e.g., ['Text', 'Metadata']).
            extensions (List[str]): List of file extensions to filter files (e.g., ['.txt', '.xml']).
            unpack (bool): Whether to unpack .gz files after download. Defaults to True.

        Returns:
            The path to the downloaded item directory, or None if no files were downloaded.
        """
        logger.info(f"Accessing IA Item: {identifier}")
        item = get_item(identifier)

        # Determine which files to download
        files_to_get = []
        for f in item.files:
            name = f['name']
            if extensions and any(name.endswith(ext) for ext in extensions):
                files_to_get.append(name)
            elif formats and f.get('format') in formats:
                files_to_get.append(name)

        if not files_to_get:
            logging.info("No matching files found for {identifier}")
            return None

        logger.debug(f"Found {len(files_to_get)} matching files for {identifier}")

        # IA subfolder setup
        item_dir = os.path.join(self.base_dir, identifier)

        # Download files
        # verbose=True for built-in progress bar
        item.download(files=files_to_get, destdir=self.base_dir, verbose=True, retries=5)

        if unpack:
            for filename in files_to_get:
                if filename.endswith(".gz"):
                    full_path = os.path.join(item_dir, filename)
                    self._unpack_gzip(full_path)

        logger.info(f"Finished downloading {identifier}. Cool-down delay...")
        time.sleep(self.delay)
        return item_dir

    def _unpack_gzip(self, file_path):
        unpacked_path = file_path.replace(".gz", "")
        logger.debug(f"Unpacking {file_path} to {unpacked_path}")
        try:
            with gzip.open(file_path, 'rb') as f_in:
                with open(unpacked_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.remove(file_path)
        except Exception as e:
            logger.debug(f"Failed to unpack {file_path}: {e}")
