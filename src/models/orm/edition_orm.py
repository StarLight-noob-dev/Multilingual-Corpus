from typing import Optional, List, Dict, Any, override

from sqlalchemy import String, Integer, Boolean
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import ARRAY

from src.database.base import Base


class EditionORM(Base):
    """
    ORM model for an Edition entity.
    Aligned with EditionRecord dataclass.
    """
    __tablename__ = "editions"

    # Identifiers
    ol_id: Mapped[str] = mapped_column(String, primary_key=True)
    ocaid: Mapped[str] = mapped_column(String, index=True)

    # Title & Metadata
    title: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    local_path: Mapped[Optional[str]] = mapped_column(String, nullable=True, default=None)

    # Publishing Dates
    publishing_date_raw: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    publishing_date: Mapped[int] = mapped_column(Integer)
    is_approximate: Mapped[bool] = mapped_column(Boolean, server_default="false", default=False)

    # Collections
    authors: Mapped[List[str]] = mapped_column(ARRAY(String), default=list)
    languages: Mapped[List[str]] = mapped_column(ARRAY(String), default=list)
    isbn_10: Mapped[List[str]] = mapped_column(ARRAY(String), default=list)
    isbn_13: Mapped[List[str]] = mapped_column(ARRAY(String), default=list)

    # Notes: Consider to define Many-to-Many relationships with authors table in future iterations
    # for more complex queries.

    def as_dict(self) -> Dict[str, Any]:
        return {
            "ol_id": self.ol_id,
            "ocaid": self.ocaid,
            "title": self.title,
            "authors": self.authors,
            "publishing_date_raw": self.publishing_date_raw,
            "publishing_date": self.publishing_date,
            "is_approximate": self.is_approximate,
            "languages": self.languages,
            "isbn_10": self.isbn_10,
            "isbn_13": self.isbn_13,
            "local_path": self.local_path
        }

    @override
    def __repr__(self) -> str:
        return f"EditionORM(ol_id={self.ol_id}, ocaid={self.ocaid}, title={self.title}, path={self.local_path})"