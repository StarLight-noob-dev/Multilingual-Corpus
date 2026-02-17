from typing import Optional, List, override, Dict

from sqlalchemy import String, Integer, Boolean, Enum as SQLEnum, Float
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.ext.mutable import MutableDict
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
    file_uri: Mapped[List[str]] = mapped_column(ARRAY(String), default=list)
    stages: Mapped[Dict[str, object]] = mapped_column(
        MutableDict.as_mutable(JSONB),
        default=dict,
        nullable=False
    )
    copyright_info: Mapped[Dict[str, object]] = mapped_column(
        MutableDict.as_mutable(JSONB),
        default=dict,
        nullable=False
    )

    # Publishing Dates
    publishing_date: Mapped[Dict[str, object]] = mapped_column(
        MutableDict.as_mutable(JSONB),
        default=dict,
        nullable=False
    )

    # Collections
    authors: Mapped[List[str]] = mapped_column(ARRAY(String), default=list)
    languages: Mapped[List[str]] = mapped_column(ARRAY(String), default=list)
    isbn_10: Mapped[List[str]] = mapped_column(ARRAY(String), default=list)
    isbn_13: Mapped[List[str]] = mapped_column(ARRAY(String), default=list)

    # --- ACL.2025 fields ---
    # Maps to JSON/JSONB to store dictionary structures
    temporal_estimates: Mapped[Dict[str, int]] = mapped_column(JSONB, default=dict)
    median_year: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    confidence_score: Mapped[float] = mapped_column(Float, default=0.0)
    is_refined_subset: Mapped[bool] = mapped_column(Boolean, default=False)

    structural_statistics: Mapped[Dict[str, int]] = mapped_column(JSONB, default=dict)

    # Notes: Consider to define Many-to-Many relationships with authors table in future iterations
    # for more complex queries.

    @override
    def __repr__(self) -> str:
        return f"EditionORM(ol_id={self.ol_id}, ocaid={self.ocaid}, title={self.title}, path={self.local_path})"