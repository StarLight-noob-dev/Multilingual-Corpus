from typing import Optional, List, Type, Iterable, Any

from sqlalchemy import select, update, delete, Select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import scoped_session

from src.common.types import T_DOMAIN, T_ORM, T_ID
from src.mappers import BaseMapper
from .repository_interface import IRepository


class BaseSqlRepository(IRepository[T_DOMAIN, T_ORM, T_ID]):
    """
    Concrete implementation of a repository for generic CRUD operations using SQLAlchemy and a Mapper to translate
    between ORM (internal) models and domain models (external).
    """

    def __init__(self,
                 session_factory: scoped_session ,
                 mapper: Type[BaseMapper[T_DOMAIN, T_ORM]],
                 ):
        """
        Initialize the repository with a SQLAlchemy session factory and model.

        Args:
            session_factory (scoped_session): SQLAlchemy scoped_session factory for obtaining Session instances.
            mapper (Type[BaseMapper[T_DOMAIN, T_ORM]]): Mapper class for converting between domain and ORM models.
        """
        self.session_factory = session_factory
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
        session = self.session_factory()
        try:
            orm = self.mapper.to_orm(entity)
            session.add(orm)
            session.commit()
            session.refresh(orm)
            return self.mapper.to_domain(orm)
        except Exception:
            session.rollback()
            raise
        finally:
            self.session_factory.remove()

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
        with self.session_factory() as session:
            orm_dict = [self.mapper.to_orm(e).to_dict() for e in entities]
            stmt = self.model.__table__.insert()
            if conflict_index:
                stmt = insert(self.model).values(orm_dict).on_conflict_do_nothing(
                    index_elements=conflict_index
                )
                session.execute(stmt)
                session.commit()
            else:
                session.execute(
                    stmt,
                    orm_dict
                )
                session.commit()
            return []

    def get_by_id(self, entity_id: T_ID) -> Optional[T_DOMAIN]:
        """
        Return an entity by its id or None if not found.

        Args:
            entity_id (T_ID): The primary key of the entity to retrieve.

        Returns:
            Optional[T_DOMAIN]: The domain model instance if found, else None.
        """
        session = self.session_factory()
        try:
            orm_entity = session.get(self.model, entity_id)
            return self.mapper.to_domain(orm_entity) if orm_entity else None
        finally:
            self.session_factory.remove()

    def get_many_by_ids(self, entity_ids: List[T_ID]) -> List[T_DOMAIN]:
        """
        Return multiple entities by their ID's or None if none found.

        Args:
            entity_ids (List[T_ID]): List of primary keys of the entities to retrieve.

        Returns:
            List[T_DOMAIN]: List of domain model instances if found, else an empty List.
        """
        if not entity_ids: return []
        session = self.session_factory()
        try:
            pk_column = getattr(self.model, self.pk_name)
            stmt = select(self.model).where(pk_column.in_(entity_ids))
            orm_entities = session.scalars(stmt).all()
            return [self.mapper.to_domain(e) for e in orm_entities]
        finally:
            self.session_factory.remove()

    def get_all(self) -> List[T_DOMAIN]:
        """
        Return all entities.

        Returns:
            List[T_DOMAIN]: List of all domain model instances.
        """
        session = self.session_factory()
        try:
            orm_entities: List[T_ORM] = list(session.scalars(select(self.model)).all())
            return [self.mapper.to_domain(e) for e in orm_entities]
        finally:
            self.session_factory.remove()

    def stream_all(self, batch_size: int = 100) -> Iterable[T_DOMAIN]:
        """
        Self-sufficient streaming.
        Note: The session stays open until the generator is finished.

        Args:
            batch_size (int): Number of records to fetch per batch from the database. Default is 100.

        Returns:
            Iterable[T_DOMAIN]: An iterable of domain model instances for all records in the database.
        """
        session = self.session_factory()
        try:
            stmt = select(self.model).execution_options(yield_per=batch_size, stream_results=True)
            for orm_entity in session.scalars(stmt):
                domain_obj = self.mapper.to_domain(orm_entity)
                try:
                    session.expunge(orm_entity)
                except Exception:
                    # If expunge fails for any reason, continue.
                    pass
                yield domain_obj
        finally:
            self.session_factory.remove()

    def stream_statement(self, stmt: Select, batch_size: int = 100) -> Iterable[T_DOMAIN]:
        """
        Streams results for a given SQLAlchemy Select statement.

        Note: The session stays open until the generator is finished.

        It applies stream_results + yield_per for server-side batching. Then expunges ORM instances to prevent
        memory growth.

        Args:
            stmt (Select): A SQLAlchemy Select statement to execute and stream results from.
            batch_size (int): Number of records to fetch per batch from the database. Default is

        Returns:
            Iterable[T_DOMAIN]: An iterable of domain model instances resulting from the executed statement.
        """
        session = self.session_factory()
        try:
            streaming_stmt = stmt.execution_options(
                yield_per=batch_size,
                stream_results=True,
            )
            for orm_entity in session.scalars(streaming_stmt):
                domain_obj = self.mapper.to_domain(orm_entity)
                try:
                    session.expunge(orm_entity)
                except Exception:
                    # If expunge fails for any reason, continue.
                    pass
                yield domain_obj
        finally:
            self.session_factory.remove()

    def update(self, entity_id: T_ID, **fields: Any) -> Optional[T_DOMAIN]:
        """
        Update an entity by its ID with the provided fields.

        Args:
            entity_id (T_ID): The primary key of the entity to update.
            **fields (Any): Fields to update on the entity.

        Returns:
            Optional[T_DOMAIN]: The updated domain model instance if found, else None.
        """
        session = self.session_factory()
        try:
            stmt = (
                update(self.model)
                .where(getattr(self.model, self.pk_name) == entity_id)
                .values(**fields)
                .execution_options(synchronize_session="fetch")
            )
            session.execute(stmt)
            session.commit()
            # Nested call to get_by_id is safe because it also calls .remove()
            return self.get_by_id(entity_id)
        except Exception:
            session.rollback()
            raise
        finally:
            self.session_factory.remove()

    def update_entity(self, entity: T_DOMAIN) -> T_DOMAIN:
        """
        Updates the database record using the provided domain entity's current state.

        Args:
            entity (T_DOMAIN): The domain model containing updated values.

        Returns:
            T_DOMAIN: The refreshed domain model from the database.
        """
        session = self.session_factory()
        try:
            orm_update = self.mapper.to_orm(entity)
            merged_orm = session.merge(orm_update)
            session.commit()
            session.refresh(merged_orm)
            return self.mapper.to_domain(merged_orm)
        except Exception:
            session.rollback()
            raise
        finally:
            self.session_factory.remove()

    def create_or_update_many(
            self,
            entities: Iterable[T_DOMAIN],
            conflict_index: List[str],
            update_columns: Optional[List[str]] = None
    ) -> None:
        """
        Perform a high-performance UPSERT for a batch of entities.
        """
        session = self.session_factory()
        try:
            orm_dicts = [self.mapper.to_orm(e).to_dict() for e in entities]
            if not orm_dicts:
                return
            stmt = insert(self.model).values(orm_dicts)
            if update_columns is None:
                # Automatic: Update everything that isn't part of the conflict index or PK
                pk_keys = self.model.__table__.primary_key.columns.keys()
                update_columns = [
                    c.name for c in self.model.__table__.columns
                    if c.name not in conflict_index and c.name not in pk_keys
                ]
            set_dict = {col: stmt.excluded[col] for col in update_columns}
            upsert_stmt = stmt.on_conflict_do_update(
                index_elements=conflict_index,
                set_=set_dict
            )
            session.execute(upsert_stmt)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            self.session_factory.remove()

    def delete(self, entity_id: T_ID) -> bool:
        """
        Delete an entity by its ID.

        It's recommended to call get_by_id after deletion to confirm removal.

        Args:
            entity_id (T_ID): The primary key of the entity to delete.

        Returns:
            bool: True if no exceptions were raised during deletion.
        """
        session = self.session_factory()
        try:
            stmt = delete(self.model).where(getattr(self.model, self.pk_name) == entity_id)
            session.execute(stmt)
            session.commit()
            return True
        except Exception:
            session.rollback()
            raise
        finally:
            self.session_factory.remove()

    def count(self) -> int:
        """
        Count the total number of entities in the repository.

        Returns:
            int: The total count of entities.
        """
        session = self.session_factory()
        try:
            return session.query(self.model).count()
        finally:
            self.session_factory.remove()