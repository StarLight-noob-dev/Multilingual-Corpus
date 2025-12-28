from src.models.orm import EditionORM
from src.models.record import EditionRecord


class EditionMapper:

    @staticmethod
    def to_domain(orm_entity: EditionRecord | list[EditionRecord]) -> EditionRecord | list[EditionRecord]:
        if isinstance(orm_entity, list):
            return [EditionMapper._single_to_domain(orm) for orm in orm_entity]
        else:
            return EditionMapper._single_to_domain(orm_entity)

    @staticmethod
    def _single_to_domain(orm: EditionRecord) -> EditionRecord:
        return EditionRecord(
            ol_id=orm.ol_id,
            ocaid=orm.ocaid,
            title=orm.title,
            publishing_date_raw=orm.publishing_date_raw,
            publishing_date=orm.publishing_date,
            is_approximate=orm.is_approximate,
            authors=orm.authors,
            languages=orm.languages,
            isbn_10=orm.isbn_10,
            isbn_13=orm.isbn_13,
            local_path=orm.local_path
        )

    @staticmethod
    def to_orm(domain_entity: EditionRecord | list[EditionRecord]) -> EditionORM | list[EditionORM]:
        if isinstance(domain_entity, list):
            return [EditionMapper._single_to_orm(record) for record in domain_entity]
        else:
            return EditionMapper._single_to_orm(domain_entity)

    @staticmethod
    def _single_to_orm(domain: EditionRecord) -> EditionORM:
        return EditionORM(
            ol_id=domain.ol_id,
            ocaid=domain.ocaid,
            title=domain.title,
            publishing_date_raw=domain.publishing_date_raw,
            publishing_date=domain.publishing_date,
            is_approximate=domain.is_approximate,
            authors=domain.authors,
            languages=domain.languages,
            isbn_10=domain.isbn_10,
            isbn_13=domain.isbn_13,
            local_path=domain.local_path
        )
