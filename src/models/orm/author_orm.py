from typing import Optional, override

from sqlalchemy import String, Integer, Boolean
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
    birth_date: Mapped[int] = mapped_column(Integer, default=-1)
    death_date: Mapped[int] = mapped_column(Integer, default=-1)
    death_date_raw: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    is_death_date_exact: Mapped[bool] = mapped_column(Boolean, server_default="false", default=False)

    @override
    def __repr__(self) -> str:
        return f"AuthorORM(ol_id={self.ol_id}, name={self.name})"