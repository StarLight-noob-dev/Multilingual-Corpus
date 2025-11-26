from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String, Integer, Boolean

from src.database.base import Base


class AuthorORM(Base):
    __tablename__ = "authors"

    _ol_id: Mapped[str] = mapped_column("id", String, primary_key=True)
    name: Mapped[str] = mapped_column("name", String, nullable=False)
    birth_date: Mapped[int] = mapped_column("birth_date", Integer, nullable=False, default=-1)
    death_date: Mapped[int] = mapped_column("death_date", Integer, nullable=False, default=-1)
    is_death_date_exact: Mapped[bool] = mapped_column("death_exact", Boolean, default=False)
    _work_count: Mapped[int] = mapped_column("work_count", Integer, default=0)
