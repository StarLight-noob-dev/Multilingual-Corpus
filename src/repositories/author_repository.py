from typing import List

from sqlalchemy.orm import sessionmaker

from src.mappers import AuthorMapper
from src.models.orm import AuthorORM
from src.models.record import AuthorRecord
from src.repositories import BaseSqlRepository


class AuthorRepository(BaseSqlRepository[AuthorRecord, AuthorORM, str]):
    def __init__(self, session_factory: sessionmaker):
        super().__init__(
            session_factory=session_factory,
            mapper=AuthorMapper
        )

    def bulk_insert(self, records: List[AuthorRecord]) -> None:
        self.create_many(
            records,
            conflict_index=['ol_id']
        )
