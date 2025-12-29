from abc import abstractmethod
from dataclasses import dataclass
from typing import Generic, Optional, List, Iterable

from src.common.types import T_DOMAIN, T_ORM, T_ID


@dataclass
class IRepository(Generic[T_DOMAIN, T_ORM, T_ID]):
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
    def get_by_id(self, entity_id: T_ID) -> Optional[T_DOMAIN]:
        """Return one ORM entity or None."""
        ...

    @abstractmethod
    def get_all(self) -> List[T_DOMAIN]:
        """Return all ORM entities."""
        ...

    # ---------------------- UPDATE ----------------------
    @abstractmethod
    def update(self, entity_id: T_ID, **fields) -> Optional[T_DOMAIN]:
        """Update an ORM entity and return the updated instance."""
        ...

    # ---------------------- DELETE ----------------------
    @abstractmethod
    def delete(self, entity_id: T_ID) -> bool:
        """Delete an ORM entity. Returns True if deleted."""
        ...
