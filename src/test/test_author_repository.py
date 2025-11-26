
from src.database.database import get_test_engine
import pytest

from sqlalchemy.orm import sessionmaker

from src.database.base import Base
from src.database.repositories.author_repository import AuthorRepository
from src.models.record.author_record import AuthorRecord


@pytest.fixture
def session():
    # In-memory SQLite for fast, isolated tests
    engine = get_test_engine()
    # Only create the authors table to avoid PostgreSQL-specific ARRAY columns on EditionORM
    Base.metadata.create_all(bind=engine, tables=[AuthorRecord.__table__])
    SessionLocal = sessionmaker(bind=engine)
    sess = SessionLocal()
    try:
        yield sess
    finally:
        sess.close()


def _insert_author(session, ol_id: str, name: str = "Name", death_date: int = 2000, is_exact: bool = False, work_count: int = 0):
    a = AuthorRecord(_ol_id=ol_id, name=name, death_date=death_date, is_death_date_exact=is_exact, _work_count=work_count)
    session.add(a)
    session.commit()
    session.refresh(a)
    return a


class TestAuthorRepository:
    @pytest.fixture(autouse=True)
    def _setup(self, session):
        # Attach the session and repository to the test instance and ensure a clean table
        self.session = session
        self.repo = AuthorRepository(session=self.session)
        # Ensure clean slate
        self.session.query(AuthorRecord).delete()
        self.session.commit()
        yield
        # Teardown: remove any authors created by the test so following tests start clean
        self.session.query(AuthorRecord).delete()
        self.session.commit()

    def test_get_many_by_ids_returns_entities(self):
        _insert_author(self.session, "A1", name="Alice")
        _insert_author(self.session, "A2", name="Bob")

        res = self.repo.get_many_by_ids(["A1", "A2"])

        assert isinstance(res, list)
        assert len(res) == 2
        ids = {r._ol_id for r in res}
        assert ids == {"A1", "A2"}

    def test_get_many_by_ids_empty_and_missing_return_none(self):
        # No authors created yet
        assert self.repo.get_many_by_ids([]) is None
        assert self.repo.get_many_by_ids(["NOPE"]) is None

        # Create one author and verify missing id still returns None when only missing
        _insert_author(self.session, "A1")
        assert self.repo.get_many_by_ids(["NOPE"]) is None

    def test_get_many_by_ids_partial_returns_existing(self):
        _insert_author(self.session, "A1")
        _insert_author(self.session, "A2")

        # Mixed existing + non-existing should return the existing ones (count > 0)
        res = self.repo.get_many_by_ids(["A1", "MISSING"])
        assert isinstance(res, list)
        assert len(res) == 1
        assert res[0].ol_id == "A1"
