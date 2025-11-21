from src.database.orm_mapper import (
    record_to_orm,
    orm_to_record,
    AuthorORM,
    EditionORM,
)
from src.models.record.author_record import AuthorRecord
from sqlalchemy import text
from src.database import database


class TestORMRoundtrip:

    def setup_method(self):
        """Create temporary test tables in the live database used for this test run.

        The tables are named `test_author` and `test_edition`. We execute
        explicit CREATE TABLE statements so we don't depend on the ORM's
        metadata (which was declared with different table names).
        """
        self.engine = database.engine

        # DDL for the two test tables - keep schema aligned with ORM mapping
        create_author = text(
            """
            CREATE TABLE IF NOT EXISTS test_author (
                ol_id VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL,
                birth_date INTEGER NOT NULL,
                death_date INTEGER NOT NULL,
                is_death_date_exact BOOLEAN NOT NULL DEFAULT false,
                work_count INTEGER DEFAULT 0
            )
            """
        )

        create_edition = text(
            """
            CREATE TABLE IF NOT EXISTS test_edition (
                ol_id VARCHAR PRIMARY KEY,
                ocaid VARCHAR NOT NULL,
                title VARCHAR NOT NULL,
                publishing_date INTEGER,
                copyright_date INTEGER,
                authors TEXT[] NOT NULL DEFAULT '{}',
                languages TEXT[] NOT NULL DEFAULT '{}',
                isbn_10 TEXT[] NOT NULL DEFAULT '{}',
                isbn_13 TEXT[] NOT NULL DEFAULT '{}',
                works TEXT[] NOT NULL DEFAULT '{}'
            )
            """
        )

        # Execute creation inside a transaction
        with self.engine.begin() as conn:
            conn.execute(create_author)
            conn.execute(create_edition)

    def teardown_method(self, method):
        """Drop the temporary tables after the test finishes."""
        drop_author = text("DROP TABLE IF EXISTS test_author CASCADE")
        drop_edition = text("DROP TABLE IF EXISTS test_edition CASCADE")
        with self.engine.begin() as conn:
            conn.execute(drop_author)
            conn.execute(drop_edition)

    def test_author_roundtrip(self):
        """Test mapping logic using a live DB table: insert a row into
        `test_author`, read it back, convert to record and compare.
        """
        # Use the dataclass record -> ORM object to build the payload
        author = AuthorRecord(_ol_id="A:1", name="Jane Doe", birth_date=1970, death_date=2020, is_death_date_exact=True)
        author.add_work(5)

        # Convert record to an ORM instance (in-memory)
        orm_obj = record_to_orm(author)
        assert isinstance(orm_obj, AuthorORM)

        # Insert into the live test table using bound params
        insert_sql = text(
            "INSERT INTO test_author (ol_id, name, birth_date, death_date, is_death_date_exact, work_count)"
            " VALUES (:ol_id, :name, :birth_date, :death_date, :is_death_date_exact, :work_count)"
        )

        params = {
            "ol_id": orm_obj.ol_id,
            "name": orm_obj.name,
            "birth_date": orm_obj.birth_date,
            "death_date": orm_obj.death_date,
            "is_death_date_exact": orm_obj.is_death_date_exact,
            "work_count": orm_obj.work_count,
        }

        with self.engine.begin() as conn:
            conn.execute(insert_sql, params)

            # Read the row back
            sel = conn.execute(text("SELECT ol_id, name, birth_date, death_date, is_death_date_exact, work_count FROM test_author WHERE ol_id = :ol_id"), {"ol_id": orm_obj.ol_id})
            row = sel.mappings().first()

        assert row is not None, "Inserted row not found in test_author table"

        # Create an AuthorORM instance from the DB row and convert back to record
        db_author = AuthorORM(
            ol_id=row["ol_id"],
            name=row["name"],
            birth_date=row["birth_date"],
            death_date=row["death_date"],
            is_death_date_exact=row["is_death_date_exact"],
            work_count=row["work_count"],
        )

        round_tripped = orm_to_record(db_author)
        assert isinstance(round_tripped, AuthorRecord)
        assert round_tripped.as_dict() == author.as_dict()

        # Ensure EditionORM can still be instantiated from parameters (sanity)
        e = EditionORM(ol_id="E:1", ocaid="oca1", title="T", publishing_date=-1, copyright_date=-1, authors=[],
                       languages=[], isbn_10=[], isbn_13=[], works=[])
        assert isinstance(e, EditionORM)
