from abc import abstractmethod, ABC
from typing import Generic, Optional, List, Type, Iterable, Dict, Any, TypeVar, TypeAlias, Callable
from sqlalchemy import select, update, values, delete
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session


T_ORM = TypeVar("T_ORM")  # ORM Model Type
T_DOMAIN = TypeVar("T_DOMAIN")  # Domain Model Type
ID = TypeVar("ID")  # Primary key type

class GenericRepository(ABC, Generic[T_DOMAIN, T_ORM, ID]):
    """
    Concrete implementation of a repository for generic CRUD operations using SQLAlchemy and a Mapper to translate
    between ORM (internal) models and domain models (external).
    """
    def __init__(self,
                 session: Session,
                 orm_model: Type[T_ORM],
                 to_domain_mapper: TypeAlias = Callable[[T_ORM | List[T_ORM]], T_DOMAIN | List[T_DOMAIN]],
                 to_orm_mapper: TypeAlias = Callable[[T_DOMAIN | List[T_DOMAIN]], T_ORM | List[T_ORM]]
                 ):
        """
        Initialize the repository with a SQLAlchemy session and model.

        Args:
            session (Session): SQLAlchemy session for database operations.
            orm_model (Type[T_ORM]): The specific SQLAlchemy ORM model class this repository manages.
            to_domain_mapper (Callable): Function that converts T_ORM -> T_DOMAIN.
            from_domain_mapper (Callable): Function that converts T_DOMAIN -> T_ORM.
        """
        self.session = session
        self.model = orm_model
        self.to_domain = to_domain_mapper
        self.to_orm = to_orm_mapper
        # Get primary key name from the model's mapper
        self.pk_name = self.model.__mapper__.primary_key[0].name

    def create(self, entity: T_DOMAIN) -> T_DOMAIN:
        """
        Create a new entity bt mapping T_DOMAIN to T_ORM and inserting it into the database.

        Args:
            entity (T_DOMAIN): The domain model instance to create.

        Returns:
            T_DOMAIN: The created domain model instance with updated fields (e.g., ID).
        """
        orm = self.to_orm(entity)
        self.session.add(orm)
        self.session.commit()
        self.session.refresh(orm)
        return self.to_domain(orm)


    def create_many(self, entities: Iterable[T_DOMAIN], conflict_index: Optional[List[str]] = None) -> None:
        """
        Batch insert multiple entities efficiently using PostgreSQL's ON CONFLICT.
        # TODO cahnge value type to Iterable[T] and see how to insert that way, also see if it should return the inserted entities
        """
        orm_dict = [self.to_orm(e).__dict__ for e in entities]

        if conflict_index:
            stmt = stmt.on_conflict_do_nothing(index_elements=conflict_index)
        self.session.execute(stmt)
        self.session.commit()


    def get_by_id(self, entity_id: ID) -> Optional[T_DOMAIN]:
        """Return one entity by its ID or None."""
        orm_entity = self.session.get(self.model, entity_id)
        if orm_entity is None:
            return None
        return self.to_domain(orm_entity)

    def get_all(self) -> List[T_DOMAIN]:
        """Return all entities."""
        stmt = select(self.model)
        orm_entities: List[T_ORM] = list(self.session.scalars(stmt).all())
        return [self.to_domain(e) for e in orm_entities]

    def update(self, entity_id: ID, **fields: Any) -> Optional[T_DOMAIN]:
        """Update an entity by its ID and return the updated T_DOMAIN."""
        stmt = (
            update(self.model)
            .where(getattr(self.model, self.pk_name) == entity_id)
            .values(**fields)
        )
        result = self.session.execute(stmt)
        if result.rowcount == 0:
            self.session.rollback()
            return None
        self.session.commit()
        return self.get_by_id(entity_id)

    def delete(self, entity_id: ID) -> bool:
        stmt = delete(self.model).where(getattr(self.model, self.pk_name) == entity_id)
        result = self.session.execute(stmt)
        if result.rowcount == 0:
            self.session.rollback()
            return False
        self.session.commit()
        return True
