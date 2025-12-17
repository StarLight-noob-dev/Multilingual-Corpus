from typing import List

from sqlalchemy.orm import Session

from src.repositories.base_repository import GenericRepository
from src.mappers.author_mapper import AuthorMapper
from src.models.orm.author_orm import AuthorORM
from src.models.record.author_record import AuthorRecord


class AuthorRepository(GenericRepository[AuthorRecord, AuthorORM, str]):
    def __init__(self, session: Session):
        super().__init__(
            session=session,
            orm_model=AuthorORM,
            to_domain_mapper=AuthorMapper.to_domain,
            to_orm_mapper=AuthorMapper.to_orm
        )

    def bulk_insert(self, records: List[AuthorRecord]) -> None:
        self.create_many(
            records,
            conflict_index=['ol_id']
        )
