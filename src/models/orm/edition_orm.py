from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String, Integer
from sqlalchemy.dialects.postgresql import JSONB

from src.database.base import Base


class EditionORM(Base):
    __tablename__ = "editions"

    _ol_id: Mapped[str] = mapped_column("id", String, primary_key=True)
    _ocaid: Mapped[str] = mapped_column(String, nullable=False)
    title: Mapped[str] = mapped_column(String)
    publishing_date: Mapped[int] = mapped_column(Integer)
    copyright_date: Mapped[int] = mapped_column(Integer)
    authors: Mapped[list[str]] = mapped_column(JSONB, default=list)
    languages: Mapped[list[str]] = mapped_column(JSONB, default=list)
    isbn_10: Mapped[list[str]] = mapped_column(JSONB, default=list)
    isbn_13: Mapped[list[str]] = mapped_column(JSONB, default=list)
    works: Mapped[list[str]] = mapped_column(JSONB, default=list)
