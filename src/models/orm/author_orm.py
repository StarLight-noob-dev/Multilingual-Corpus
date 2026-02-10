from typing import override, Dict

from sqlalchemy import String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import Mapped, mapped_column

from src.database.base import Base


class AuthorORM(Base):
    """
    ORM model for an Author entity.
    Aligned with AuthorRecord dataclass.
    """
    __tablename__ = "authors"

    # Primary Key
    ol_id: Mapped[str] = mapped_column(String, primary_key=True)

    # Core Fields
    name: Mapped[str] = mapped_column(String, nullable=False)

    # Date Information
    birth_date: Mapped[Dict[str, object]] = mapped_column(
        MutableDict.as_mutable(JSONB),
        default=dict,
        nullable=False
    )

    death_date: Mapped[Dict[str, object]] = mapped_column(
        MutableDict.as_mutable(JSONB),
        default=dict,
        nullable=False
    )

    @override
    def __repr__(self) -> str:
        return f"AuthorORM(ol_id={self.ol_id}, name={self.name})"