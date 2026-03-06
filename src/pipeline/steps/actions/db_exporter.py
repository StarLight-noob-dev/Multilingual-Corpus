import logging
import threading
from typing import override, List

from src.common.types import T_DOMAIN, T_ORM, T_ID
from src.pipeline.steps import BaseAction
from src.repositories import BaseSqlRepository


logger = logging.getLogger("BufferedPostgresExporter")

class BufferedPostgresExporter(BaseAction):
    """
    Export data to a PostgreSQL database using buffered inserts for efficiency.

    Note: self.flush() should be called to ensure all buffered data is written to the database before
    the program exits or when you want to ensure all data is saved.
    """

    def __init__(self, repository: BaseSqlRepository[T_DOMAIN, T_ORM, T_ID], buffer_size: int = 1000):
        """
        Initialize the BufferedPostgresExporter.

        Args:
            repository : The repository instance for database operations.
            buffer_size (int): Number of records to buffer before inserting into the database.
        """
        self.repo = repository
        self.buffer_size = buffer_size
        self.buffer = []
        self._lock = threading.Lock()

    @override
    def perform(self, data: T_DOMAIN) -> T_DOMAIN:
        to_flush = None
        with self._lock:
            self.buffer.append(data)
            if len(self.buffer) >= self.buffer_size:
                to_flush = self.buffer
                self.buffer = []
        if to_flush:
            self._flush_buffer(to_flush)
        return data

    def flush(self) -> None:
        """Flush the buffered records to the database."""
        to_flush = None
        with self._lock:
            if self.buffer:
                to_flush = self.buffer
                self.buffer = []
        if to_flush:
            self._flush_buffer(to_flush)

    def _flush_buffer(self, records: List[T_DOMAIN]) -> None:
        """Helper method to flush a given list of records to the database."""
        if not records:
            return

        try:
            if hasattr(self.repo, 'bulk_insert'):
                self.repo.bulk_insert(records)
            elif hasattr(self.repo, 'create_many'):
                key = self.repo.mapper.ORM_CLASS.__table__.primary_key.columns.keys()
                self.repo.create_many(records, conflict_index=key)
            else:
                for record in records:
                    self.repo.create(record)
        except Exception as e:
            logger.exception(f"Failed to flush buffer to database: {e}")


class BufferedPostgresUpdater(BufferedPostgresExporter):
    """
    Export data to a PostgreSQL database using buffered updates for efficiency.

    Note: self.flush() should be called to ensure all buffered data is written to the database before
    the program exits or when you want to ensure all data is saved.
    """
    @override
    def _flush_buffer(self, records: List[T_DOMAIN]) -> None:
        """Helper method to flush a given list of records to the database."""
        if not records:
            return

        try:
            if hasattr(self.repo, 'create_or_update_many'):
                key = self.repo.mapper.ORM_CLASS.__table__.primary_key.columns.keys()
                self.repo.create_or_update_many(records, conflict_index=key)
            elif hasattr(self.repo, 'bulk_update'):
                self.repo.bulk_update(records)
            else:
                for record in records:
                    self.repo.update(record)
        except Exception as e:
            logger.exception(f"Failed to flush buffer to database: {e}")