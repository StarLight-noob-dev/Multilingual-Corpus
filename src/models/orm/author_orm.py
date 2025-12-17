from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String, Integer, Boolean

from src.database.base import Base


class AuthorORM(Base):
    """
    ORM model for an Author entity in the database.
    
    Attributes:
        _ol_id (str): The Open Library ID of the author (primary key).
        name (str): The name of the author.
        birth_date (int): The birthdate of the author (year).
        death_date (int): The death date of the author (year).
        is_death_date_exact (bool): Flag indicating if the death date is exact.
        _work_count (int): The number of works associated with the author.
    """
    __tablename__ = "authors"

    ol_id: Mapped[str] = mapped_column("ol_id", String, primary_key=True)
    name: Mapped[str] = mapped_column("name", String, nullable=False)
    birth_date: Mapped[int] = mapped_column("birth_date", Integer, nullable=False, default=-1)
    death_date: Mapped[int] = mapped_column("death_date", Integer, nullable=False, default=-1)
    is_death_date_exact: Mapped[bool] = mapped_column("death_exact", Boolean, default=False)
    _work_count: Mapped[int] = mapped_column("work_count", Integer, default=0)

    def to_dict(self):
        return {
            "ol_id": self.ol_id,
            "name": self.name,
            "birth_date": self.birth_date,
            "death_date": self.death_date,
            "is_death_date_exact": self.is_death_date_exact,
            "work_count": self._work_count
        }