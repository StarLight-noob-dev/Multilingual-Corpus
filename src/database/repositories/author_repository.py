from typing import List, Optional

from sqlalchemy.orm import Session

from src.database.orm_mapper import AuthorORM
from src.database.repositories.base_repository import BaseRepository


class AuthorRepository(BaseRepository[AuthorORM, str]):
    """
    Concrete repository for AuthorORM entities.
    Inherits all CRUD operations from BaseRepository.
    """

    def __init__(self, session: Session):
        super().__init__(session=session, model=AuthorORM)

    def get_many_by_ids(self, author_ids: List[str]) -> Optional[List[AuthorORM]]:
        """Retrieve multiple authors by their IDs."""
        result = [self.get_by_id(id) for id in author_ids]
        return result if any(result) else None
