from dataclasses import fields, asdict
from typing import Generic, Type, List, Dict, Any

from src.common.types import T_DOMAIN, T_ORM
from src.models.record import EditionRecord, AuthorRecord
from src.models.orm import EditionORM, AuthorORM


class BaseMapper(Generic[T_DOMAIN, T_ORM]):
    """Generic base mapper for converting between domain records and ORM entities."""

    DOMAIN_CLASS: Type[T_DOMAIN]
    ORM_CLASS: Type[T_ORM]

    # Use a dictionary to keep caches separated by class type
    _FIELD_REGISTRY: Dict[Type[Any], List[str]] = {}

    @classmethod
    def _get_domain_fields(cls) -> List[str]:
        if cls not in cls._FIELD_REGISTRY:
            cls._FIELD_REGISTRY[cls] = [f.name for f in fields(cls.DOMAIN_CLASS)]
        return cls._FIELD_REGISTRY[cls]

    @classmethod
    def to_domain(cls, orm: T_ORM) -> T_DOMAIN:
        if orm is None:
            return None
        target = cls._get_domain_fields()
        data = {f: getattr(orm, f) for f in target}
        return cls.DOMAIN_CLASS(**data)

    @classmethod
    def to_orm(cls, domain: T_DOMAIN) -> T_ORM:
        if domain is None:
            return None
        data = domain.as_dict() if hasattr(domain, 'as_dict') else asdict(domain)
        return cls.ORM_CLASS(**data)


class EditionMapper(BaseMapper[EditionRecord, EditionORM]):
    DOMAIN_CLASS = EditionRecord
    ORM_CLASS = EditionORM


class AuthorMapper(BaseMapper[AuthorRecord, AuthorORM]):
    DOMAIN_CLASS = AuthorRecord
    ORM_CLASS = AuthorORM