from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String, Integer
from sqlalchemy.dialects.postgresql import JSONB

from src.database.base import Base


class EditionORM(Base):
    """
    ORM model for an Edition entity in the database.

    Attributes:
        _ol_id (str): The Open Library ID of the edition (primary key).
        _ocaid (str): The Open Content Alliance ID of the edition.
        title (str): The title of the edition.
        publishing_date (int): The publishing date of the edition (year).
        copyright_date (int): The copyright date of the edition (year).
        authors (list[str]): List of author IDs associated with the edition.
        languages (list[str]): List of language codes for the edition.
        isbn_10 (list[str]): List of ISBN-10 codes for the edition.
        isbn_13 (list[str]): List of ISBN-13 codes for the edition.
        works (list[str]): List of work IDs associated with the edition.
    """
    __tablename__ = "editions"

    ol_id: Mapped[str] = mapped_column("ol_id", String, primary_key=True)
    ocaid: Mapped[str] = mapped_column("ocaid", String, nullable=False)
    title: Mapped[str] = mapped_column(String)
    publishing_date: Mapped[int] = mapped_column(Integer)
    copyright_date: Mapped[int] = mapped_column(Integer)
    authors: Mapped[list[str]] = mapped_column(JSONB, default=list)
    languages: Mapped[list[str]] = mapped_column(JSONB, default=list)
    isbn_10: Mapped[list[str]] = mapped_column(JSONB, default=list)
    isbn_13: Mapped[list[str]] = mapped_column(JSONB, default=list)
    works: Mapped[list[str]] = mapped_column(JSONB, default=list)
