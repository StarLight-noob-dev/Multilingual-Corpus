from typing import Optional, List, Type, Iterable, Any

from sqlalchemy import select, update, delete
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from src.common.types import T_DOMAIN, T_ORM, T_ID
from src.mappers import BaseMapper
from .repository_interface import IRepository


class BaseSqlRepository(IRepository[T_DOMAIN, T_ORM, T_ID]):
    """
    Concrete implementation of a repository for generic CRUD operations using SQLAlchemy and a Mapper to translate
    between ORM (internal) models and domain models (external).
    """

    def __init__(self,
                 session: Session,
                 mapper: Type[BaseMapper[T_DOMAIN, T_ORM]],
                 ):
        """
        Initialize the repository with a SQLAlchemy session and model.

        Args:
            session (Session): SQLAlchemy session for database operations.
            mapper (Type[BaseMapper[T_DOMAIN, T_ORM]]): Mapper class for converting between domain and ORM models.
        """
        self.session = session
        self.model = mapper.ORM_CLASS
        self.mapper = mapper
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
        orm = self.mapper.to_orm(entity)
        self.session.add(orm)
        self.session.commit()
        self.session.refresh(orm)
        return self.mapper.to_domain(orm)

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
        orm_dict = [self.mapper.to_orm(e).to_dict() for e in entities]
        stmt = self.model.__table__.insert()
        if conflict_index:
            stmt = insert(self.model).values(orm_dict).on_conflict_do_nothing(
                index_elements=conflict_index
            )
            self.session.execute(stmt)
            self.session.commit()
        else:
            self.session.execute(
                stmt,
                orm_dict
            )
            self.session.commit()
        return []

    def get_by_id(self, entity_id: T_ID) -> Optional[T_DOMAIN]:
        """
        Return an entity by its id or None if not found.

        Args:
            entity_id (T_ID): The primary key of the entity to retrieve.

        Returns:
            Optional[T_DOMAIN]: The domain model instance if found, else None.
        """
        orm_entity = self.session.get(self.model, entity_id)
        if orm_entity is None:
            return None
        return self.mapper.to_domain(orm_entity)

    def get_many_by_ids(self, entity_ids: List[T_ID]) -> List[T_DOMAIN]:
        """
        Return multiple entities by their ID's or None if none found.

        Args:
            entity_ids (List[T_ID]): List of primary keys of the entities to retrieve.

        Returns:
            List[T_DOMAIN]: List of domain model instances if found, else an empty List.
        """
        if not entity_ids:
            return []
        pk_colum = getattr(self.model, self.pk_name)
        stmt = select(self.model).where(pk_colum.in_(entity_ids))
        orm_entities: List[T_ORM] = list(self.session.scalars(stmt).all())
        return [self.mapper.to_domain(e) for e in orm_entities]

    def get_all(self) -> List[T_DOMAIN]:
        """
        Return all entities.

        Returns:
            List[T_DOMAIN]: List of all domain model instances.
        """
        stmt = select(self.model)
        orm_entities: List[T_ORM] = list(self.session.scalars(stmt).all())
        return [self.mapper.to_domain(e) for e in orm_entities]

    def update(self, entity_id: T_ID, **fields: Any) -> Optional[T_DOMAIN]:
        """
        Update an entity by its ID with the provided fields.

        Args:
            entity_id (T_ID): The primary key of the entity to update.
            **fields (Any): Fields to update on the entity.

        Returns:
            Optional[T_DOMAIN]: The updated domain model instance if found, else None.
        """
        stmt = (
            update(self.model)
            .where(getattr(self.model, self.pk_name) == entity_id)
            .values(**fields)
            .execution_options(synchronize_session="fetch")
        )
        self.session.execute(stmt)
        self.session.commit()
        updated_entity = self.get_by_id(entity_id)
        if updated_entity is None:
            return None
        return updated_entity

    def delete(self, entity_id: T_ID) -> bool:
        """
        Delete an entity by its ID.

        It's recommended to call get_by_id after deletion to confirm removal.

        Args:
            entity_id (T_ID): The primary key of the entity to delete.

        Returns:
            bool: True if no exceptions were raised during deletion.
        """
        stmt = delete(self.model).where(getattr(self.model, self.pk_name) == entity_id)
        self.session.execute(stmt)
        self.session.commit()
        return True
