from typing import Optional, List, override

from sqlalchemy import String, Integer, Boolean, Enum as SQLEnum
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column

from src.database.base import Base
from src.models.record import RecordStatus


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
    status: Mapped[RecordStatus] = mapped_column(SQLEnum(RecordStatus), default=RecordStatus.PENDING)
    error: Mapped[Optional[str]] = mapped_column(String, nullable=True, default=None)
    retries: Mapped[int] = mapped_column(Integer, server_default="0", default=0)

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

    @override
    def __repr__(self) -> str:
        return f"EditionORM(ol_id={self.ol_id}, ocaid={self.ocaid}, title={self.title}, path={self.local_path})"