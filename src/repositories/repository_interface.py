from abc import abstractmethod
from dataclasses import dataclass
from typing import (
    TypeVar, Generic, Optional, List, Iterable,
    Callable, TypeAlias
)

T_ORM = TypeVar("T_ORM")  # ORM Model Type
T_DOMAIN = TypeVar("T_DOMAIN")  # Domain Model Type
ID = TypeVar("ID")  # Primary key type

__all__ = ["IRepository", "T_ORM", "T_DOMAIN", "ID"]

@dataclass
class IRepository(Generic[T_DOMAIN, T_ORM, ID]):
    """Repository interface returning Domain Models."""

    # ---------------------- CREATE ----------------------
    @abstractmethod
    def create(self, entity: T_DOMAIN) -> T_DOMAIN:
        """Create a new ORM entity."""
        ...

    @abstractmethod
    def create_many(self, entities: Iterable[T_DOMAIN]) -> List[T_DOMAIN]:
        """Create multiple ORM entities."""
        ...

    # ---------------------- READ ------------------------
    @abstractmethod
    def get_by_id(self, entity_id: ID) -> Optional[T_DOMAIN]:
        """Return one ORM entity or None."""
        ...

    @abstractmethod
    def get_all(self) -> List[T_DOMAIN]:
        """Return all ORM entities."""
        ...

    # ---------------------- UPDATE ----------------------
    @abstractmethod
    def update(self, entity_id: ID, **fields) -> Optional[T_DOMAIN]:
        """Update an ORM entity and return the updated instance."""
        ...

    # ---------------------- DELETE ----------------------
    @abstractmethod
    def delete(self, entity_id: ID) -> bool:
        """Delete an ORM entity. Returns True if deleted."""
        ...
