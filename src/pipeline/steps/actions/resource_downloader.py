import logging

from time import sleep
from pathlib import Path
from typing import List, Optional, override, Any, Callable

import requests
from internetarchive import get_item, Item

from src.models.record import EditionRecord
from src.pipeline.steps import BaseAction
from src.utils.file_utils import unpack_gzip

logger = logging.getLogger(__name__)

# Type alias for the callback: (file_name, response_object, data_object(e.g. EditionRecord for this manager)) -> Any
IACallback = Callable[[str, requests.Response, Any], Any]

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
            delay=2.0,
            verbose=True
        )

        record = ia_downloader.perform(edition_record) # or .execute(edition_record)
    """

    AVAILABLE_FORMATS = ['DjVuTXT', 'Text PDF', 'Abbyy GZ', 'Djvu XML', 'DjVu']
    AVAILABLE_EXTENSIONS = ['.txt', '.pdf', '.gz', '.xml', '.djvu']

    def __init__(self,
                 formats: List[str] = None,
                 extensions: List[str] = None,
                 base_dir: str | Path = "downloads",
                 delay: float = 2.0,
                 verbose: bool = False,
                 callback: Optional[IACallback] = None) -> None:
        """
        Initialize the IADownloadManager.

        If multiple downloads are performed in succession, a delay can be set to avoid throttling.
        The more aggressive the download (fewer delays), the higher the chance of being throttled by IA.

        Args:
            formats (list): List of IA formats to filter files (e.g., ['Text', 'Metadata']).
            extensions (list): List of file extensions to filter files (e.g., ['.txt', '.xml']).
            base_dir (str): Directory to store downloaded files.
            delay (float): Delay in seconds between downloads to avoid throttling.
            verbose (bool): Whether to log detailed information during the download process.
            callback: Optional function to handle the stream instead of saving to disk
        """
        if not formats and not extensions:
            raise ValueError("At least one of 'formats' or 'extensions' must be specified.")

        self.formats = formats
        self.extensions = extensions
        self.base_dir = Path(base_dir)
        self.delay = delay
        self.verbose = verbose
        self.callback = callback

        if not self.callback:
            self.base_dir.mkdir(parents=True, exist_ok=True)

    @override
    def perform(self, data: EditionRecord) -> EditionRecord:
        identifier = data.ocaid
        if not identifier:  # Should not happen, but just in case
            return data

        try:
            item = get_item(identifier)
            target_files: List[str] = self._get_target_files(item)

            if not target_files:
                logger.debug(f"No matching files for {identifier}")
                data.local_path = None  # Ensures local_path is None
                return data

            if self.callback:
                self._handle_callback_stream(item, target_files, data)
            else:
                self._handle_local_download(item, target_files, data)

            sleep(self.delay)

        except Exception as e:
            logger.error(f"Manager failed for {identifier}: {e}")
            data.local_path = None

        return data

    def _handle_local_download(self, item: Item, files: List[str], data: EditionRecord) -> EditionRecord:
        """Downloads files locally to the specified base directory."""
        item.download(
            files=files, # type: ignore
            destdir=str(self.base_dir),
            retries=3,
            checksum=True,  # Verify file integrity after download
            verbose=True if self.verbose else False,
            return_responses=False
        )

        item_dir = self.base_dir / item.identifier
        found_files = [f for f in files if (item_dir / f).exists()] # Check if files were downloaded
        # Only post-process if files were actually downloaded
        if found_files:
            IADownloadManager._post_process_files(item_dir, files)
            data.local_path = str(item_dir)
        else:
            data.local_path = None
        return data

    def _handle_callback_stream(self, item: Item, files: List[str], data: EditionRecord):
        """Streams data directly from IA to the provided callback."""
        for file_name in files:
            success = False
            attempts = 0
            max_tries = 3

            while not success and attempts < max_tries:
                response = item.download(files=file_name, return_responses=True) # type: ignore
                resp = response[0] if response else None # Should only be one response

                if not resp or resp.status_code != 200:
                    attempts += 1
                    continue

                try:
                    logger.debug(f"Attempt {attempts+1}: Piping {file_name}")
                    self.callback(file_name, resp, data)
                    success = True
                except Exception as callback_err:
                    logger.warning(f"Callback failed for {file_name} on attempt {attempts+1}: {callback_err}")
                    attempts += 1
                    if attempts < max_tries:
                        sleep(2 ** attempts) # Exponential backoff
                finally:
                    resp.close() # Ensure the response is closed to free resources

            if not success:
                logger.warning(f"Failed to pipe {file_name} after {max_tries} attempts.")

    def _get_target_files(self, item: Item) -> List[str]:
        """Filter files based on specified formats or extensions."""
        targets: List[str] = []
        for f in item.files:
            # Skip private files, they can be borrowed via IA but not downloaded directly
            private = f.get('private', 'false').lower() == 'true'
            if private:
                continue

            name = f.get('name', '')
            # Match by extension or format
            if self.extensions and any(name.endswith(ext) for ext in self.extensions):
                targets.append(name)
            elif self.formats and f.get('format', '') in self.formats:
                targets.append(name)
        return targets

    @staticmethod
    def _post_process_files(item_dir: Path, file_names: List[str]) -> None:
        """Post-process downloaded files, such as unpacking .gz files."""
        for name in file_names:
            file_path = item_dir / name
            if file_path.exists():
                if file_path.name.endswith('.gz'):
                    try:
                        unpack_gzip(file_path, remove_original=True)
                        logger.debug(f"Unpacked {file_path}")
                    except Exception as e:
                        logger.warning(f"Post-processing failed for {name}: {e}")

    @staticmethod
    @DeprecationWarning
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
        item_dir = Path(directory) / (identifier)

        # Download files and use checksum to test integrity
        item.download(
            files=files_to_download,
            destdir=directory,
            verbose=True if verbose else False,
            retries=retries,
            checksum=True
        )

        if unpack:
            for filename in files_to_download:
                if filename.endswith(".gz"):
                    full_path = item_dir / filename
                    unpack_gzip(full_path)
        sleep(delay)
        return item_dir