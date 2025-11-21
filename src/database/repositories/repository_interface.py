from abc import abstractmethod
from dataclasses import dataclass
from typing import TypeVar, Generic, Optional, List, Iterable, Dict

T = TypeVar("T")  # ORM Model Type
ID = TypeVar("ID")  # Primary key type


@dataclass
class IRepository(Generic[T, ID]):
    """Minimal ORM-only CRUD interface for repositories."""
    @abstractmethod
    def create(self, **fields) -> T:
        """Create a new ORM entity."""
        ...

    @abstractmethod
    def create_many(self, entities: Iterable[Dict]) -> List[T]:
        """Create multiple ORM entities."""
        ...

    @abstractmethod
    def get_by_id(self, entity_id: ID) -> Optional[T]:
        """Return one ORM entity or None."""
        ...

    @abstractmethod
    def get_all(self) -> List[T]:
        """Return all ORM entities."""
        ...

    @abstractmethod
    def update(self, entity_id: ID, **fields) -> Optional[T]:
        """Update an ORM entity and return the updated instance."""
        ...

    @abstractmethod
    def delete(self, entity_id: ID) -> bool:
        """Delete an ORM entity. Returns True if deleted."""
        ...
