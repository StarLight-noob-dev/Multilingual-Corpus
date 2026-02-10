from dataclasses import fields, asdict
from typing import Generic, Type, List, Dict, Any

from src.common.types import T_DOMAIN, T_ORM
from src.models.record import EditionRecord, AuthorRecord, ParsedDate
from src.models.orm import EditionORM, AuthorORM


class BaseMapper(Generic[T_DOMAIN, T_ORM]):
    """
    Generic base mapper for converting between domain records and ORM entities.

    It uses dataclass field introspection to dynamically map fields between the two types.
    This means that T_DOMAIN should be a dataclass.

    Attributes:
        DOMAIN_CLASS (Type[T_DOMAIN]): The domain record class.
        ORM_CLASS (Type[T_ORM]): The ORM entity class.

    Methods:
        to_domain(orm: T_ORM) -> T_DOMAIN: Converts an ORM entity to a domain record.
        to_orm(domain: T_DOMAIN) -> T_ORM: Converts a domain record to an ORM entity.
    """

    DOMAIN_CLASS: Type[T_DOMAIN]
    ORM_CLASS: Type[T_ORM]

    # Use a dictionary to keep caches separated by class type
    _FIELD_REGISTRY: Dict[Type[Any], List[str]] = {}

    # For composition, we can change how value mapping is done by overriding these dictionaries in subclasses if needed
    # DOMAIN_TO_ORM is mostly there to change the mappings to the ORM side if necessary, but in most cases, the domain
    # record should be the source of truth for field names and types ORM_TO_DOMAIN is what allows to map complex ORM
    # fields (like JSON blobs) to more structured domain fields (like ParsedDate)
    DOMAIN_TO_ORM: Dict[str, callable] = {}
    ORM_TO_DOMAIN: Dict[str, callable] = {}

    @classmethod
    def _get_domain_fields(cls) -> List[str]:
        if cls not in cls._FIELD_REGISTRY:
            cls._FIELD_REGISTRY[cls] = [f.name for f in fields(cls.DOMAIN_CLASS)]
        return cls._FIELD_REGISTRY[cls]

    @classmethod
    def to_domain(cls, orm: T_ORM) -> T_DOMAIN:
        if orm is None:
            return None
        data = {}
        for field in cls._get_domain_fields():
            value = getattr(orm, field)
            if field in cls.ORM_TO_DOMAIN:
                value = cls.ORM_TO_DOMAIN[field](value)
            data[field] = value
        return cls.DOMAIN_CLASS(**data)

    @classmethod
    def to_orm(cls, domain: T_DOMAIN) -> T_ORM:
        if domain is None:
            return None
        raw = domain.to_dict() if hasattr(domain, 'to_dict') else asdict(domain)
        data = {}
        for field, value in raw.items():
            if field in cls.DOMAIN_TO_ORM:
                value = cls.DOMAIN_TO_ORM[field](value)
            data[field] = value
        return cls.ORM_CLASS(**data)


class EditionMapper(BaseMapper[EditionRecord, EditionORM]):
    DOMAIN_CLASS = EditionRecord
    ORM_CLASS = EditionORM


class AuthorMapper(BaseMapper[AuthorRecord, AuthorORM]):
    DOMAIN_CLASS = AuthorRecord
    ORM_CLASS = AuthorORM

    ORM_TO_DOMAIN = {
        "birth_date": lambda d: ParsedDate.from_mapped_dict(d),
        "death_date": lambda d: ParsedDate.from_mapped_dict(d)
    }

