from typing import Dict, Any

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    def to_dict(self) -> Dict[str, Any]:
        return {c.key: getattr(self, c.key) for c in self.__table__.columns}