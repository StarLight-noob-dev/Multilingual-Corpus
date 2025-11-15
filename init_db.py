import logging
from src.database.database import engine
# Importing all the models from Declarative Base
from src.database.orm_mapper import Base

logger = logging.getLogger(__name__)


def init_db():
    # Create tables
    Base.metadata.create_all(engine)
    logger.info("All tables checked/created successfully!")

if __name__ == "__main__":
    init_db()
