from src.models.orm.author_orm import AuthorORM
from src.models.orm.edition_orm import EditionORM
from src.models.record.author_record import AuthorRecord
from src.models.record.edition_record import EditionRecord


@DeprecationWarning
class RecordMapper:
    @staticmethod
    def author_to_orm(record: AuthorRecord) -> AuthorORM:
        return AuthorORM(
            ol_id=record._ol_id,
            name=record.name,
            birth_date=record.birth_date,
            death_date=record.death_date,
            is_death_date_exact=record.is_death_date_exact,
            _work_count=record.work(),
        )

    @staticmethod
    def orm_to_author(orm: AuthorORM) -> AuthorRecord:
        author = AuthorRecord(
            _ol_id=orm.ol_id,
            name=orm.name,
            birth_date=orm.birth_date,
            death_date=orm.death_date,
            is_death_date_exact=orm.is_death_date_exact,
        )
        author.add_work(orm._work_count)
        return author

    @staticmethod
    def edition_to_orm(record: EditionRecord) -> EditionORM:
        return EditionORM(
            ol_id=record._ol_id,
            ocaid=record._ocaid,
            title=record.title,
            publishing_date=record.publishing_date,
            copyright_date=record.copyright_date,
            authors=record.authors,
            languages=record.languages,
            isbn_10=record.isbn_10,
            isbn_13=record.isbn_13,
            works=record.works,
        )

    @staticmethod
    def orm_to_edition(orm: EditionORM) -> EditionRecord:
        return EditionRecord(
            _ol_id=orm._ol_id,
            _ocaid=orm._ocaid,
            title=orm.title,
            publishing_date=orm.publishing_date,
            copyright_date=orm.copyright_date,
            authors=orm.authors,
            languages=orm.languages,
            isbn_10=orm.isbn_10,
            isbn_13=orm.isbn_13,
            works=orm.works,
        )
