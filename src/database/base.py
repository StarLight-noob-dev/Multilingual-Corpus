from sqlalchemy.orm import declarative_base

Base = declarative_base() # NOTE: Now all ORM are also dataclasses, upgrade to separated + mappers if needed