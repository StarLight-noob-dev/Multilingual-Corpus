import pytest
from sqlalchemy.orm import Session

from src.database.repositories.author_repository import AuthorRepository
from src.models.orm.author_orm import AuthorORM
from src.models.record.author_record import AuthorRecord
from src.test.fixtures import session


def _insert_author(session, ol_id: str, name: str = "Name", death_date: int = 2000, is_exact: bool = False, work_count: int = 0):
    a = AuthorORM(ol_id=ol_id, name=name, death_date=death_date, is_death_date_exact=is_exact, _work_count=work_count)
    session.add(a)
    session.commit()
    session.refresh(a)
    return a


class TestAuthorRepository:

    @pytest.fixture(autouse=True)
    def setup(self, session: Session):
        self.session = session
        self.repo = AuthorRepository(session)
        self.insert_author = lambda **kwargs: _insert_author(self.session, **kwargs)

    def test_insert_valid_author_and_its_found(self):
        self.insert_author(ol_id="A1", name="Luz")
        self.session.commit()
        res = self.repo.get_by_id("A1")

        assert isinstance(res, AuthorRecord)
        assert res.id == "A1"
        assert res.name == "Luz"

    def test_get_many_by_ids_returns_entities(self):
        self.insert_author(ol_id="A1", name="Luz")
        self.insert_author(ol_id="A2", name="Amity")

        self.session.commit()

        res = self.repo.get_many_by_ids(["A1", "A2"])

        assert isinstance(res, list)
        assert len(res) == 2
        ids = {r._ol_id for r in res}
        assert ids == {"A1", "A2"}
        assert res[0].name == "Luz" or res[1].name == "Amity"

    def test_get_many_by_ids_empty_and_missing_return_empty_list(self):
        # No authors in DB should return empty list
        res = self.repo.get_many_by_ids([])
        assert isinstance(res, list)
        assert len(res) == 0

        # Returns empty list for missing ids
        res = self.repo.get_many_by_ids(["NOPE"])
        assert isinstance(res, list)
        assert len(res) == 0

        # Create one author and verify missing still returns empty list
        self.insert_author(ol_id="A1")
        self.session.commit()
        res = self.repo.get_many_by_ids(["NOPE"])
        assert isinstance(res, list)
        assert len(res) == 0

    def test_get_many_by_ids_partial_returns_existing(self):
        self.insert_author(ol_id="A1", name="Luz")
        self.insert_author(ol_id="A2")
        self.session.commit()

        # Mixed existing + non-existing should return the existing ones (count > 0)
        res = self.repo.get_many_by_ids(["A1", "MISSING"])
        assert isinstance(res, list)
        assert len(res) == 1
        assert res[0]._ol_id == "A1"
        assert res[0].name == "Luz"
