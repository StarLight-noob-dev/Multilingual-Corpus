from typing import Generic, Optional, List, Type, Iterable, Dict
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from src.database.repositories.repository_interface import IRepository, T, ID


class BaseRepository(Generic[T, ID], IRepository[T, ID]):
    """
    A type-safe base repository for ORM models.
    Provides CRUD operations using SQLAlchemy ORM only.
    """

    def __init__(self, session: Session, model: Type[T]):
        self.session = session
        self.model = model
        self.pk = model.__mapper__.primary_key[0]

    # ---------------------- CREATE ----------------------
    def create(self, **fields) -> T:
        entity = self.model(**fields)
        self.session.add(entity)
        self.session.commit()
        self.session.refresh(entity)
        return entity

    def create_many(self, values: Iterable[Dict], conflict_index: Optional[List[str]] = None) -> None:
        """
        Batch insert multiple entities efficiently using PostgreSQL's ON CONFLICT.
        # TODO cahnge value type to Iterable[T] and see how to insert that way, also see if it should return the inserted entities
        """
        stmt = pg_insert(self.model).values(list(values))
        if conflict_index:
            stmt = stmt.on_conflict_do_nothing(index_elements=conflict_index)
        self.session.execute(stmt)
        self.session.commit()

    # ---------------------- READ ------------------------
    def get_by_id(self, entity_id: ID) -> Optional[T]:
        stmt = select(self.model).where(self.pk == entity_id) #TODO might need to be .equals()? do a test
        result = self.session.execute(stmt)
        return result

    def get_all(self) -> List[T]:
        stmt = select(self.model)
        result = self.session.execute(stmt)
        return list(result.scalars().all())

    # ---------------------- UPDATE ----------------------
    def update(self, entity_id: ID, **fields) -> Optional[T]:
        entity = self.get_by_id(entity_id)
        if not entity:
            return None
        for key, value in fields.items():
            setattr(entity, key, value)
        self.session.commit()
        self.session.refresh(entity)
        return entity

    # ---------------------- DELETE ----------------------
    def delete(self, entity_id: ID) -> bool:
        entity = self.get_by_id(entity_id)
        if not entity:
            return False
        self.session.delete(entity)
        self.session.commit()
        return True
