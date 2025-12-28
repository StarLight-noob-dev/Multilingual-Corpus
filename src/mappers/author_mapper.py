from typing import List

from src.models.orm import AuthorORM
from src.models.record import AuthorRecord


class AuthorMapper:

    @staticmethod
    def to_domain(orm_entity: AuthorORM | List[AuthorORM]) -> AuthorRecord | List[AuthorRecord]:
        if isinstance(orm_entity, list):
            return [AuthorMapper._single_to_domain(orm) for orm in orm_entity]
        else:
            return AuthorMapper._single_to_domain(orm_entity)

    @staticmethod
    def _single_to_domain(orm: AuthorORM) -> AuthorRecord:
        author = AuthorRecord(
            ol_id=orm.ol_id,
            name=orm.name,
            death_date_raw=orm.death_date_raw,
            death_date=orm.death_date,
            birth_date=orm.birth_date,
            is_death_date_exact=orm.is_death_date_exact,
        )
        return author

    @staticmethod
    def to_orm(domain_entity: AuthorRecord | List[AuthorRecord]) -> AuthorORM | List[AuthorORM]:
        if isinstance(domain_entity, list):
            return [AuthorMapper._single_to_orm(record) for record in domain_entity]
        else:
            return AuthorMapper._single_to_orm(domain_entity)

    @staticmethod
    def _single_to_orm(domain_entity: AuthorRecord) -> AuthorORM:
        return AuthorORM(
            ol_id=domain_entity.ol_id,
            name=domain_entity.name,
            birth_date=domain_entity.birth_date,
            death_date=domain_entity.death_date,
            death_date_raw=domain_entity.death_date_raw,
            is_death_date_exact=domain_entity.is_death_date_exact,
        )