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
        return AuthorRecord(
            ol_id=orm.ol_id,
            name=orm.name,
            death_date_raw=orm.death_date_raw,
            death_date=orm.death_date,
            birth_date=orm.birth_date,
            is_death_date_exact=orm.is_death_date_exact,
        )

    @staticmethod
    def to_orm(domain_entity: AuthorRecord | List[AuthorRecord]) -> AuthorORM | List[AuthorORM]:
        if isinstance(domain_entity, list):
            return [AuthorMapper._single_to_orm(record) for record in domain_entity]
        else:
            return AuthorMapper._single_to_orm(domain_entity)

    @staticmethod
    def _single_to_orm(domain: AuthorRecord) -> AuthorORM:
        return AuthorORM(
            ol_id=domain.ol_id,
            name=domain.name,
            birth_date=domain.birth_date,
            death_date=domain.death_date,
            death_date_raw=domain.death_date_raw,
            is_death_date_exact=domain.is_death_date_exact,
        )