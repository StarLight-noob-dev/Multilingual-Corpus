import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.database.orm_mapper import Base

# Load environment variables from .env file
load_dotenv()

# --- Load database config from environment (.env values or defaults) ---
DB_USER = os.getenv("DB_USER", "thesis_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "thesis_password")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "thesis_db")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
TEST_DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}_test"

# --- Create SQLAlchemy engine ---
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# For multiprocessing safety, we create engines/sessions on demand per process
_engine = None
_SessionLocal = None

def get_session_maker():
    """
    Returns a new session-maker instance.

    This functions ensures that get an independent session-maker with its own engine for each
    process, avoiding cross-process connection sharing issues.

    Meant to be used in the DB related stages of multiprocessing.Pool workers.
    """
    global _engine, _SessionLocal
    if _engine is None:
        _engine = create_engine(
            DATABASE_URL,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
            echo=False
        )
        _SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=_engine)
    return _SessionLocal

def init_db():
    """Initializes the database connection and creates the tables based on the ORM models."""
    engine_ = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine_)
    engine_.dispose()

def init_test_db():
    """Initializes the test database connection and creates the tables based on the ORM models."""
    engine_ = create_engine(TEST_DATABASE_URL)
    Base.metadata.create_all(engine_)
    engine_.dispose()

def get_test_engine():
    """Returns a SQLAlchemy engine for the test database."""
    return create_engine(TEST_DATABASE_URL)

def get_test_sessionmaker():
    """Returns a sessionmaker for the test database."""
    return sessionmaker(autocomit=False, autoflush=False, bind=get_test_engine())