import logging
import multiprocessing as mp
from itertools import islice
from pathlib import Path
from typing import List, Any, Iterable

from src.config.paths import DataPaths
from src.models.file_chunk import Chunk
from src.models.record.transport_record import TransportRecord

logger = logging.getLogger(__name__)


class DumpReader:

    @staticmethod
    def get_file_chunks(file_name: str | Path, max_cpu: int = 16) -> List[Chunk]:
        """
        Splits a file into chunks for parallel processing or batch processing.

        Args:
            file_name (str): Path to the file to be chunked.
            max_cpu (int): Maximum number of CPU cores to use, the default is 16, and it uses the
                minimum between this value and the available CPU cores.

        Returns:
            List[Chunk]: A list of Chunk objects representing the file chunks.
        """
        path = Path(file_name)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_name}")

        cpu_count = min(max_cpu, mp.cpu_count())
        if cpu_count < 1:
            raise ValueError("At least one CPU core is required for processing.")
        file_size = path.stat().st_size
        chunk_size = file_size // cpu_count

        chunks: List[Chunk] = []
        with path.open("rb") as f:
            start = 0
            while start < file_size:
                end = min(start + chunk_size, file_size)

                # If not at EOF, move end to the next newline to avoid splitting a record
                if end < file_size:
                    f.seek(end)
                    f.readline()
                    end = f.tell()

                chunks.append(Chunk(file_name=str(path), start=start, end=end))
                start = end

        return chunks

    @staticmethod
    def record_generator(file_name: str | Path, start: int, end: int) -> Iterable[TransportRecord]:
        """
        Reads a specific byte range from a file and yields TransportRecords.

        Args:
            file_name (str | Path): Path to the file.
            start (int): The byte to start reading from.
            end (int): The byte where the generator should stop.

        Returns:
            Yields a TransportRecord for each valid record in the chunk containing the id and the JSON data and
            type of record.
        """
        path = Path(file_name)
        with path.open("r", encoding="utf-8") as f:
            f.seek(start)
            while f.tell() < end:
                line = f.readline()
                if not line:
                    break
                parts = line.strip().split("\t")
                if len(parts) == 5:
                    yield TransportRecord(
                        r_type=parts[0].strip(),
                        ol_id=parts[1].strip(),
                        json_string=parts[4].strip()
                    )
                else:
                    logger.warning(f"Malformed line at offset {f.tell()}")

    @staticmethod
    def batch_generator[T](iterable: Iterable[T], size: int) -> Iterable[List[T]]:
        """
        Yields batches of size batch_size from the input generator.

        Args:
            iterable (Iterable[Any]): Input generator yielding individual elements.
            size (int): Size of each batch.

        Yields:
            List[Any]: A batch containing batch_size elements or what is left in case
            there is fewer elements than the batch size.
        """
        it = iter(iterable)
        while batch := list(islice(it, size)):
            yield batch

    @classmethod
    def process_file(cls, file_name: str | Path, batch_size: int = None) -> (Iterable[list[TransportRecord]]
                                                                 | Iterable[TransportRecord]):
        """
        Process the entire file and return either an iterator of TransportRecord or
        batches of TransportRecords depending on `batch_size`.

        Args:
            file_name (str): Path to the file to be processed.
            batch_size (int, optional): Size of each batch. If None or less than 2,
            returns an iterator over individual TransportRecord.
        """
        path = Path(file_name)
        gen = cls.record_generator(file_name, 0, path.stat().st_size)
        if batch_size is not None and batch_size > 1:
            return cls.batch_generator(gen, batch_size)
        else:
            return gen

    @classmethod
    def get_edition_generator(cls, batch_size: int = None) -> Iterable[Any]:
        return cls.process_file(DataPaths.EDITION_DUMP, batch_size)

    @classmethod
    def get_author_generator(cls, batch_size: int = None) -> Iterable[Any]:
        return cls.process_file(DataPaths.AUTHOR_DUMP, batch_size)

    @classmethod
    def get_edition_sample_generator(cls, size: str = "small", batch_size: int = None) -> Iterable[Any]:
        """
        Get a generator for sample edition dumps of specified size.

        Args:
            size (str): Size of the sample ('small', 'medium', 'big').
            batch_size (int, optional): Size of each batch. If None or less than 2,
            returns an iterator over individual TransportRecord.

        Yields:
            Iterable[Any]: An iterator or batch generator of TransportRecords.
        """
        sample_file = {
            "small": DataPaths.SMALL_EDITION_SAMPLE,
            "medium": DataPaths.MEDIUM_EDITION_SAMPLE,
            "big": DataPaths.BIG_EDITION_SAMPLE
        }.get(size.lower(), DataPaths.SMALL_EDITION_SAMPLE)
        return DumpReader.process_file(sample_file, batch_size)

    @classmethod
    def get_author_sample_generator(cls, size: str = "small", batch_size: int = None) -> Iterable[Any]:
        """
        Get a generator for sample author dumps of specified size.

        Args:
            size (str): Size of the sample ('small', 'medium', 'big').
            batch_size (int, optional): Size of each batch. If None or less than 2,
            returns an iterator over individual TransportRecord.

        Yields:
            Iterable[Any]: An iterator or batch generator of TransportRecords.
        """
        sample_file = {
            "small": DataPaths.SMALL_AUTHOR_SAMPLE,
            "medium": DataPaths.MEDIUM_AUTHOR_SAMPLE,
            "big": DataPaths.BIG_AUTHOR_SAMPLE
        }.get(size.lower(), DataPaths.SMALL_AUTHOR_SAMPLE)
        return DumpReader.process_file(sample_file, batch_size)