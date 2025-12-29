from typing import List

from sqlalchemy.orm import Session

from src.repositories import BaseSqlRepository
from src.mappers import AuthorMapper
from src.models.orm import AuthorORM
from src.models.record import AuthorRecord


class AuthorRepository(BaseSqlRepository[AuthorRecord, AuthorORM, str]):
    def __init__(self, session: Session):
        super().__init__(
            session=session,
            mapper=AuthorMapper
        )

    def bulk_insert(self, records: List[AuthorRecord]) -> None:
        self.create_many(
            records,
            conflict_index=['ol_id']
        )
