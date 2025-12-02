from abc import ABC
from typing import Generic, Optional, List, Type, Iterable, Any, TypeVar, TypeAlias, Callable

from sqlalchemy import select, update, delete
from sqlalchemy.dialects.postgresql import insert
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


    def create_many(self, entities: Iterable[T_DOMAIN], conflict_index: Optional[List[str]] = None) -> List[T_DOMAIN]:
        """
        Create multiple entities in a batch operation with optional conflict handling.

        Args:
            entities (Iterable[T_DOMAIN]): An iterable of domain model instances to persist.
            conflict_index (Optional[List[str]]): List of column names to use for conflict detection. If provided,
            conflicts will be ignored. (e.g., _ol_id for most models)

        Returns:
            List[T_DOMAIN]: An empty list (as per current implementation).
        """
        orm_dict = [self.to_orm(e).__dict__ for e in entities]
        stmt = self.model.__table__.insert()
        if conflict_index:
            stmt = insert(self.model).on_conflict_do_nothing(
                index_elements=conflict_index
            )
            self.session.execute(stmt)
        else:
            self.session.execute(
                stmt,
                orm_dict
            )
            self.session.commit()
        return []

    def get_by_id(self, entity_id: ID) -> Optional[T_DOMAIN]:
        """
        Return an entity by its ID or None if not found.

        Args:
            entity_id (ID): The primary key of the entity to retrieve.

        Returns:
            Optional[T_DOMAIN]: The domain model instance if found, else None.
        """
        orm_entity = self.session.get(self.model, entity_id)
        if orm_entity is None:
            return None
        return self.to_domain(orm_entity)

    def get_many_by_ids(self, entity_ids: List[ID]) -> List[T_DOMAIN]:
        """
        Return multiple entities by their IDs or None if none found.

        Args:
            entity_ids (List[ID]): List of primary keys of the entities to retrieve.

        Returns:
            List[T_DOMAIN]: List of domain model instances if found, else an empty List.
        """
        if not entity_ids:
            return []
        stmt = select(self.model).where(getattr(self.model, self.pk_name) in entity_ids)
        orm_entities: List[T_ORM] = list(self.session.scalars(stmt).all())
        return [self.to_domain(e) for e in orm_entities]

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
        if result.rowcount == 0: #TODO: verify if this works as intended, linter marks as problematic
            self.session.rollback()
            return False
        self.session.commit()
        return True
