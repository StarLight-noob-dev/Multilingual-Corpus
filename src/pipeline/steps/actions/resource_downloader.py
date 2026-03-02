import datetime
import logging

from time import sleep
from pathlib import Path
from typing import List, Optional, override, Any, Callable

import requests
from internetarchive import get_session, Item

from src.models.record import EditionRecord, RecordStatus, StageInfo
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
        self.ia_session = get_session(config={
            'retries': 3,
            'pool_connections': 16,
            'pool_maxsize': 16,
            'timeout': 30
        })
        if not self.callback:
            self.base_dir.mkdir(parents=True, exist_ok=True)

    @override
    def perform(self, data: EditionRecord) -> EditionRecord:
        identifier = data.ocaid
        if not identifier:
            msg = "No identifier"
            st = StageInfo(status=RecordStatus.OMIT, message=msg)
            data.stages['book_download'] = st
            return data

        try:
            item = self.ia_session.get_item(identifier)
            target_files: List[str] = self._get_target_files(item)

            if not target_files:
                logger.debug(f"No matching files for {identifier}")
                msg = f"No public files found for specified formats={self.formats} or extensions={self.extensions}."
                st = StageInfo(status=RecordStatus.COMPLETED, message=msg)
                data.stages['book_download'] = st
                return data

            data.stages['book_download'] = StageInfo(
                status=RecordStatus.IN_PROGRESS,
                message=f"Starting download of {len(target_files)} files.",
                timestamp=str(datetime.datetime.now())
            )

            if self.callback:
                self._handle_callback_stream(item, target_files, data)
            else:
                self._handle_local_download(item, target_files, data)

            st = data.stages['book_download']
            if st and st.status == RecordStatus.IN_PROGRESS:
                new_st = st.with_status(RecordStatus.COMPLETED).with_message(st.message + " Download completed.")
                data.stages['book_download'] = new_st

            sleep(self.delay)

        except RecursionError as rec_err:
            logger.error(f"Recursion error for {identifier}: {rec_err}")
            raise RecursionError(f"Recursion error for {identifier}: {rec_err}")

        except Exception as e:
            if "429" in str(e) or "limit" in str(e).lower() or "throttle" in str(e).lower():
                raise
            logger.error(f"Manager failed for {identifier}: {e}")
            msg = f"Manager Error: {str(e)} | Type: {type(e).__name__}"
            new_st = StageInfo(status=RecordStatus.ERROR, message=msg, timestamp=str(datetime.datetime.now()))
            data.stages['book_download'] = new_st

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
            data.file_uri.extend(found_files)
            msg = f"Downloaded {len(found_files)} files."
            st = StageInfo(status=RecordStatus.COMPLETED, message=msg)
            data.stages['book_download'] = st
        return data

    def _handle_callback_stream(self, item: Item, files: List[str], data: EditionRecord):
        """Streams data directly from IA to the provided callback."""
        for file_name in files:
            st = data.stages.get('book_download', None)
            try:
                response = item.download(
                    files=file_name, # type: ignore
                    return_responses=True,
                    timeout=30,
                    retries=3,
                    verbose=True if self.verbose else False,
                )
                resp = response[0] if response else None # Should only be one response

                if not resp or resp.status_code != 200:
                    logger.warning(f"Failed to download {file_name} for {item.identifier}.")
                    msg = f" [{file_name} : {resp.status_code if resp else 'No Response'}]"
                    if st:
                        new_st = st.with_status(RecordStatus.ERROR).with_message(st.message + msg)
                    else:
                        new_st = StageInfo(status=RecordStatus.ERROR, message=msg, timestamp=str(datetime.datetime.now()))
                    data.stages['book_download'] = new_st

                    if resp.status_code == 429 or (resp.status_code == 503 and "limit" in (resp.text or "").lower()):
                        raise Exception(f"Throttled by IA with status {resp.status_code}.")
                    continue

                self.callback(file_name, resp, data)

            except Exception as callback_err:
                logger.error(f"Stream error: {callback_err}")
                msg = f"Callback Error: {str(callback_err)} | Type: {type(callback_err).__name__}"
                if st:
                    new_st = st.with_status(RecordStatus.ERROR).with_message(st.message + f" [{file_name}: {msg}]")
                else:
                    new_st = StageInfo(status=RecordStatus.ERROR, message=msg, timestamp=str(datetime.datetime.now()))
                data.stages['book_download'] = new_st

            finally:
                if 'resp' in locals() and resp:
                    resp.close() # Ensure the response is closed to free resources

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

    @override
    def execute(self, data: Any) -> Optional[Any]:
        try:
            self.perform(data)
            return data
        except Exception as e:
            if "429" in str(e) or "limit" in str(e).lower() or "throttle" in str(e).lower():
                raise
            logger.error(f"Action {self.__class__.__name__} failed: \n\t[*] Data: {data} \n\t[*] Error: {e}")
            return None
