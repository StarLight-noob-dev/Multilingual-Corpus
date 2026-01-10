import logging
import os
import time
from pathlib import Path
from typing import List, Optional, override
from internetarchive import get_item

from src.models.record import EditionRecord
from src.pipeline.steps import BaseAction

logger = logging.getLogger(__name__)


class IADownloadManager(BaseAction):
    """
    Action to download files from Internet Archive based on specified formats or extensions.
    This action uses the `internetarchive` Python package to interact with IA.

    Some common IA formats include: DjVuTXT, Text PDF, Abbyy GZ, Djvu XML, DjVu.

    Common file extensions include: .txt, .pdf, .gz, .xml, .djvu.

    Example usage::

        ia_downloader = IADownloadManager(
            formats=['DjVuTXT', 'Text PDF'],
            extensions=['.txt', '.pdf'],
            base_dir='downloads',
            delay=2.0
        )

        record = ia_downloader.perform(edition_record) # or .execute(edition_record)

        # Using the static method directly
        path_to_files = IADownloadManager.download(
            identifier='example_identifier',
            formats=['DjVuTXT', 'Text PDF'],
            extensions=['.txt', '.pdf'],
            directory='downloads',
            unpack=True,
            retries=3,
            verbose=True,
            delay=2.0
        )
    """

    AVAILABLE_FORMATS = ['DjVuTXT', 'Text PDF', 'Abbyy GZ', 'Djvu XML', 'DjVu']
    AVAILABLE_EXTENSIONS = ['.txt', '.pdf', '.gz', '.xml', '.djvu']

    def __init__(self,
                 formats: List[str],
                 extensions: List[str],
                 base_dir: str | Path = "downloads",
                 delay: float = 2.0) -> None:
        """
        Initialize the IADownloadManager.

        If multiple downloads are performed in succession, a delay can be set to avoid throttling.
        The more aggressive the download (fewer delays), the higher the chance of being throttled by IA.

        Args:
            formats (list): List of IA formats to filter files (e.g., ['Text', 'Metadata']).
            extensions (list): List of file extensions to filter files (e.g., ['.txt', '.xml']).
            base_dir (str): Directory to store downloaded files.
            delay (float): Delay in seconds between downloads to avoid throttling.
        """
        self.formats = formats
        self.extensions = extensions
        self.base_dir = base_dir
        self.delay = delay
        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir)

    @override
    def perform(self, data: EditionRecord) -> EditionRecord:
        try:
            path_to_file = self._download(
                identifier=data.ocaid,
                unpack=True,
                retries=3,
                verbose=False
            )
        except Exception as e:
            logger.debug(f"Download failed for {data.ocaid}: {e}")
            path_to_file = None

        data.local_path = str(path_to_file) if path_to_file else None
        return data

    def _download(self,
                  identifier: str,
                  unpack: bool = True,
                  retries: int = 3,
                  verbose: bool = False
                  ) -> Optional[Path]:
        return self.download(
            identifier=identifier,
            formats=self.formats,
            extensions=self.extensions,
            directory=self.base_dir,
            unpack=unpack,
            retries=retries,
            verbose=verbose,
            delay=self.delay
        )

    @staticmethod
    def download(
            identifier: str,
            formats: List[str] = None,
            extensions: List[str] = None,
            directory: str | Path = None,
            unpack: bool = True,
            retries: int = 3,
            verbose: bool = False,
            delay: float = 2.0
    ) -> Optional[Path]:
        """
        Download files from an Internet Archive item based on specified formats or extensions for a given identifier.

        Args:
            identifier (str): The Internet Archive item identifier.
            formats (list): List of IA formats to filter files (e.g., ['Text', 'Metadata']).
            extensions (list): List of file extensions to filter files (e.g., ['.txt', '.xml']).
            unpack (bool): Whether to unpack .gz files after download. Defaults to True.
            retries (int): Number of retries for downloading files in case of failure.
            verbose (bool): Whether to log detailed information during the download process.

        Returns:
            The path to the downloaded item directory, or None if no files were downloaded.
        """
        if not formats and not extensions:
            raise ValueError("At least one of 'formats' or 'extensions' must be specified.")

        if not identifier:
            raise ValueError("An 'identifier' must be specified.")

        item = get_item(identifier)

        files_to_download = []
        for f in item.files:
            # Skip private files, they can be borrowed via IA but not downloaded directly
            private = f.get('private', 'false').lower() == 'true'
            if private:
                continue
            name = f.get('name', '')
            if extensions and any(name.endswith(ext) for ext in extensions):
                files_to_download.append(name)
            elif formats and f.get('format', '') in formats:
                files_to_download.append(name)

        if not files_to_download:
            if verbose:
                logging.info(f"No files found for {identifier}")
            return None

        # IA subfolder setup
        item_dir = Path(directory)/(identifier)

        # Download files and use checksum to test integrity
        item.download(
            files=files_to_download,
            destdir=directory,
            verbose=False,
            retries=retries,
            checksum=True
        )

        if unpack:
            for filename in files_to_download:
                if filename.endswith(".gz"):
                    full_path = item_dir / filename
                    IADownloadManager._unpack_gzip(full_path)

        if verbose:
            logger.info(f"Downloaded item to {item_dir}. Waiting for {delay} seconds to avoid throttling.")

        time.sleep(delay)
        return item_dir

    @staticmethod
    def _unpack_gzip(file_path: str | Path) -> None:
        unpacked_file_path = Path(file_path).replace(".gz", "")
        logger.debug(f"Unpacking {file_path} to {unpacked_file_path}")
        try:
            import gzip
            import shutil

            with gzip.open(file_path, 'rb') as f_in:
                with open(unpacked_file_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        except Exception as e:
            logger.debug(f"Failed to unpack {file_path}: {e}")