from typing import List

from sqlalchemy.orm import scoped_session

from src.mappers import EditionMapper
from src.models.orm import EditionORM
from src.models.record import EditionRecord
from src.repositories import BaseSqlRepository


class EditionRepository(BaseSqlRepository[EditionRecord, EditionORM, str]):
    def __init__(self, session_factory: scoped_session):
        super().__init__(
            session_factory=session_factory,
            mapper=EditionMapper
        )

    def bulk_insert(self, records: List[EditionRecord]) -> None:
        self.create_many(
            records,
            conflict_index=['ol_id']
        )

    def bulk_update(self, records: List[EditionRecord]) -> None:
        self.update_many(
            records,
            conflict_index=['ol_id']
        )