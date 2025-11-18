import os
import logging
import multiprocessing as mp
from pathlib import Path
from typing import List, Tuple, Any, Iterable

from src.models.file_chunk import Chunk
from src.models.record.transport_record import TransportRecord

logger = logging.getLogger(__name__)


class DumpReader:
    _CURRENT_DIR = Path(__file__).resolve().parent.parent.parent
    _EDITION_DUMP_NAME = "data\\raw\\ol_dump_editions_latest.txt"
    _WORK_DUMP_NAME = "data\\raw\\ol_dump_works_latest.txt"
    _AUTHOR_DUMP_NAME = "data\\raw\\ol_dump_authors_latest.txt"
    _EDITION_BIG = "data\\samples\\big_editions.txt"
    _EDITION_MEDIUM = "data\\samples\\medium_editions.txt"
    _EDITION_SMALL = "data\\samples\\small_editions.txt"

    @staticmethod
    def _ensure_exists(file_name: str) -> None:
        if not os.path.isfile(file_name):
            logger.error(f"File {file_name} does not exist")

    @staticmethod
    def get_file_chunks(file_name: str, max_cpu: int = 16) -> Tuple[int, int, List[Chunk]]:
        """
        Splits a file into chunks for parallel processing.

        Args:
            file_name (str): Path to the file to be chunked.
            max_cpu (int): Maximum number of CPU cores to use, the default is 16, and it uses the
            minimum between this value and the available CPU cores.

        Returns:
            Tuple[int, int, List[Chunk]]: A tuple (int, int, [Chunk]) containing the number of CPU core
            used, the amount of chunks created, and a list of Chunk objects representing the file chunks.
        """
        logger.debug(f"Accessing {os.path.abspath(file_name)}")
        DumpReader._ensure_exists(file_name)

        cpu_count = min(max_cpu, mp.cpu_count())
        if cpu_count < 1:
            raise ValueError("At least one CPU core is required for processing.")

        file_size = os.path.getsize(file_name)
        chunk_size = file_size // cpu_count
        chunks: List[Chunk] = []

        logger.debug(f"File size: {file_size} bytes. Using {cpu_count} CPU cores with chunk size {chunk_size} bytes.")
        logger.info(f"Splitting file {file_name} into chunks...")

        try:
            with open(file_name, mode="r+b") as f:

                def is_new_line(position):
                    if position == 0:
                        return True
                    else:
                        f.seek(position - 1)
                        return f.read(1) == b'\n'

                def next_line(position):
                    f.seek(position)
                    f.readline()
                    return f.tell()

                chunk_start = 0
                while chunk_start < file_size:
                    chunk_end = min(file_size, chunk_start + chunk_size)

                    while chunk_end > 0 and not is_new_line(chunk_end):
                        chunk_end -= 1

                    if chunk_start == chunk_end:
                        chunk_end = next_line(chunk_end)

                    chunks.append(
                        (
                            Chunk(
                                file_name,
                                chunk_start,
                                chunk_end
                            )
                        )
                    )
                    chunk_start = chunk_end
        except Exception as e:
            logger.error(f"Error while splitting file into chunks: {e}")
            raise e
        finally:
            f.close()

        logger.info(f"File split into {len(chunks)} chunks.")
        return cpu_count, len(chunks), chunks

    @staticmethod
    def record_from_chunk_gen(file_name: str, chunk_start: int, chunk_end: int) -> Iterable[TransportRecord]:
        """
        Generator that yields a TransportRecord for each valid record in the specified chunk of the file.

        Args:
            file_name (str): Path to the file.
            chunk_start (int): Start byte of the chunk.
            chunk_end (int): End byte of the chunk.

        Returns:
            Yields a TransportRecord for each valid record in the chunk containing the id and the JSON data and
            type of record.
        """
        try:
            with open(file_name, encoding="utf-8", mode='r') as f:
                f.seek(chunk_start)
                for i, line in enumerate(f):
                    chunk_start += len(line)
                    if chunk_start > chunk_end:
                        break

                    # OL data dumps are TSV formatted and JSON is in the 5th column. (type, id, revision, timestamp, json)
                    line = line.strip()
                    if not line:
                        continue
                    parts = line.split("\t")

                    if len(parts) != 5:
                        logger.debug(
                            f"Malformed line at record {i} in chunk {chunk_start}-{chunk_end} in file {file_name}.")
                        continue

                    yield TransportRecord(r_type=parts[0], _ol_id=parts[1], json_string=parts[4])
        finally:
            try:
                f.close()
            except Exception:
                pass

    @staticmethod
    def batch_generator[T](generator: Iterable[T], batch_size: int) -> Iterable[List[T]]:
        """
        Yields batches of size batch_size from the input generator.

        Args:
            generator (Iterable[Any]): Input generator yielding individual elements.
            batch_size (int): Size of each batch.

        Yields:
            List[Any]: A batch containing batch_size elements or what is left in case
            there is fewer elements than the batch size.
        """
        batch = []
        for item in generator:
            batch.append(item)
            if len(batch) == batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    @staticmethod
    def process_file(file_name: str, batch_size: int = None) -> (Iterable[list[TransportRecord]]
                                                                 | Iterable[TransportRecord]):
        """
        Process the entire file and return either an iterator of TransportRecord or
        batches of TransportRecords depending on `batch_size`.

        Args:
            file_name (str): Path to the file to be processed.
            batch_size (int, optional): Size of each batch. If None or less than 2,
            returns an iterator over individual TransportRecord.
        """
        file_size = os.path.getsize(file_name)
        gen = DumpReader.record_from_chunk_gen(file_name, 0, file_size)
        if batch_size is not None and batch_size > 1:
            return DumpReader.batch_generator(gen, batch_size)
        else:
            return gen

    def get_edition_generator(self, batch_size: int = None) -> Iterable[Any]:
        return DumpReader.process_file(os.path.join(self._CURRENT_DIR, self._EDITION_DUMP_NAME), batch_size)

    def get_author_generator(self, batch_size: int = None) -> Iterable[Any]:
        return DumpReader.process_file(os.path.join(self._CURRENT_DIR, self._AUTHOR_DUMP_NAME), batch_size)

    def get_work_generator(self, batch_size: int = None) -> Iterable[Any]:
        return DumpReader.process_file(os.path.join(self._CURRENT_DIR, self._WORK_DUMP_NAME), batch_size)

    def get_edition_sample_generator(self, size: str = "small", batch_size: int = None) -> Iterable[Any]:
        sample_file = {
            "small": self._EDITION_SMALL,
            "medium": self._EDITION_MEDIUM,
            "big": self._EDITION_BIG
        }.get(size.lower(), self._EDITION_SMALL)
        return DumpReader.process_file(os.path.join(self._CURRENT_DIR, sample_file), batch_size)