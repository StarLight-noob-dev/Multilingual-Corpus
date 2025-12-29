from typing import List

from sqlalchemy.orm import Session

from src.mappers import EditionMapper
from src.models.orm import EditionORM
from src.models.record import EditionRecord
from src.repositories import BaseSqlRepository


class EditionRepository(BaseSqlRepository[EditionRecord, EditionORM, str]):
    def __init__(self, session: Session):
        super().__init__(
            session=session,
            mapper=EditionMapper
        )

    def bulk_insert(self, records: List[EditionRecord]) -> None:
        self.create_many(
            records,
            conflict_index=['ol_id']
        )