import pytest
from sqlalchemy import String, Integer
from sqlalchemy.orm import Mapped, mapped_column, Session

from src.database.base import Base


# A simple ORM Model for testing
class TestEntityORM(Base):
    __tablename__ = "test_entities"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    value: Mapped[str] = mapped_column(String)


# A simple Domain Model for testing
class TestEntityDomain:
    def __init__(self, id: int, value: str):
        self.id = id
        self.value = value

    def __eq__(self, other):
        if not isinstance(other, TestEntityDomain):
            return False
        return self.id == other.id and self.value == other.value

    def __hash__(self):
        return hash((self.id, self.value))

    def __repr__(self):
        return f"TestEntityDomain(id={self.id}, value='{self.value}')"


# --- 2. SETUP: Concrete Test Repository (Test functionality from GenericRepository) ---
from src.repositories.base_repository import GenericRepository


class TestRepo(GenericRepository[TestEntityDomain, TestEntityORM, int]):
    # These class attributes must be set for the generic methods to work

    def __init__(self, session: Session):
        super().__init__(
            session=session,
            orm_model=TestEntityORM,
            to_domain_mapper=self.to_domain,
            to_orm_mapper=self.to_orm
        )

    def to_domain(self, orm_entity: TestEntityORM) -> TestEntityDomain:
        return TestEntityDomain(id=orm_entity.id, value=orm_entity.value)

    def to_orm(self, domain_entity: TestEntityDomain) -> TestEntityORM:
        return TestEntityORM(id=domain_entity.id, value=domain_entity.value)


# --- 3. TEST CLASS: Test Suite for Generic Repository Methods ---

class TestGenericRepositoryMethods:
    repo: TestRepo
    session: Session

    @pytest.fixture(autouse=True)
    def setup_repository(self, session: Session):
        """Initializes the repository and provides access to the transactional session."""
        self.repo = TestRepo(session=session)
        self.session = session

    def _insert_entity(self, entity_id: int, value: str):
        """Helper to insert and commit data for test visibility."""
        entity_orm = TestEntityORM(id=entity_id, value=value)
        self.session.add(entity_orm)
        self.session.commit()  # Commit is crucial for visibility in subsequent queries
        self.session.expunge_all()  # Clear session cache for clean query testing

    def test_get_by_id_returns_entity(self):
        self._insert_entity(entity_id=1, value="Initial Value")

        result = self.repo.get_by_id(1)

        assert result is not None
        assert result.id == 1
        assert result.value == "Initial Value"
        assert isinstance(result, TestEntityDomain)

    def test_get_by_id_returns_none_if_not_found(self):
        result = self.repo.get_by_id(999)
        assert result is None

    def test_get_many_by_ids_returns_correct_entities(self):
        self._insert_entity(entity_id=10, value="A")
        self._insert_entity(entity_id=20, value="B")
        self._insert_entity(entity_id=30, value="C")

        # IDs to search for, including one that doesn't exist (99)
        ids_to_find = [10, 30, 99]
        expected_entities = [
            TestEntityDomain(id=10, value="A"),
            TestEntityDomain(id=30, value="C")
        ]

        results = self.repo.get_many_by_ids(ids_to_find)

        assert isinstance(results, list)
        assert len(results) == 2
        # Compare sets for order-independent check
        assert set(results) == set(expected_entities)

    def test_get_many_by_ids_returns_empty_list_for_empty_input(self):
        results = self.repo.get_many_by_ids([])
        assert results == []

    def test_create_adds_new_entity(self):
        new_entity = TestEntityDomain(id=2, value="New Entity")
        created_entity = self.repo.create(new_entity)

        assert created_entity == new_entity

        # Verify persistence (must fetch with a clean session to be sure)
        fetched_entity = self.repo.get_by_id(2)
        assert fetched_entity == new_entity

    def test_update_modifies_entity(self):
        self._insert_entity(entity_id=3, value="Old Value")

        # Update the entity
        updated_entity = self.repo.update(3, value="New Updated Value")

        assert updated_entity is not None
        assert updated_entity.value == "New Updated Value"

        # Verify persistence
        fetched_entity = self.repo.get_by_id(3)
        assert fetched_entity.value == "New Updated Value"

    def test_update_returns_none_if_not_found(self):
        updated_entity = self.repo.update(999, value="Should not happen")
        assert updated_entity is None

    def test_delete_removes_entity(self):
        self._insert_entity(entity_id=4, value="To Delete")

        deleted = self.repo.delete(4)

        assert deleted is True
        assert self.repo.get_by_id(4) is None  # Verify deletion

    def test_delete_returns_false_if_not_found(self):
        deleted = self.repo.delete(999)
        assert deleted is False