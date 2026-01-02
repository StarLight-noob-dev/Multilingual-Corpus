import threading
from typing import override

from src.common.types import T_DOMAIN, T_ORM, T_ID
from src.pipeline.steps import BaseAction
from src.repositories import BaseSqlRepository


class BufferedPostgresExporter(BaseAction):
    """
    Export data to a PostgreSQL database using buffered inserts for efficiency.
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
    def perform(self, data: T_DOMAIN) -> None:
        with self._lock:
            self.buffer.append(data)
            if len(self.buffer) >= self.buffer_size:
                self.flush()

    def flush(self) -> None:
        """Flush the buffer and insert all buffered records into the database."""
        if not self.buffer:
            return

        try:
            if hasattr(self.repo, 'bulk_insert'):
                self.repo.bulk_insert(self.buffer)
            elif hasattr(self.repo, 'create_many'):
                key = self.repo.mapper.ORM_CLASS.__table__.primary_key.columns.keys()
                self.repo.create_many(self.buffer, conflict_index=key)
            else:
                for record in self.buffer:
                    self.repo.create(record)
        except Exception as e:
            # TODO Log the error or handle it as needed
            print(f"Error during bulk insert: {e}")
        finally:
            self.buffer = []