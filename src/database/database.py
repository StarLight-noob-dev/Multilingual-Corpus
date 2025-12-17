from time import sleep

from sqlalchemy import create_engine, text, Engine
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy_utils import database_exists, create_database

from src.database.base import Base
from src.database.config import (
    DB_NAME,
    DATABASE_URL,
    TEST_DATABASE_URL,
    ADMIN_DATABASE_URL
)

# --- Create SQLAlchemy engine ---
# Standard engine creation for the main application
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=16,
    max_overflow=20,
    echo=False
)

# Standard SessionLocal setup for application use
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)


def get_db() -> Session:
    db = SessionLocal()
    return db


def init_app_db() -> None:
    print("Initializing application tables...")
    Base.metadata.create_all(bind=engine)


def get_admin_engine(echo: bool = False) -> Engine:
    """Creates and returns an admin engine connected to the default 'postgres' database."""
    return create_engine(ADMIN_DATABASE_URL, echo=echo)


def get_test_engine(max_retries: int = 5, delay_sec: int = 1, echo: bool = False) -> Engine | None:
    # Ensure the test database exists and create it if not
    if not database_exists(TEST_DATABASE_URL):
        print(f"Creating test database {TEST_DATABASE_URL}")
        create_database(TEST_DATABASE_URL)

    # Retry logic to wait for the database to be ready
    for attempt in range(max_retries):
        try:
            test_engine = create_engine(TEST_DATABASE_URL, echo=echo)
            with test_engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            print(f"Successfully connected to test database {TEST_DATABASE_URL} on attempt {attempt + 1}")
            return test_engine
        except Exception as e:
            if "database" in str(e) and "does not exist" in str(e):
                print(f"Attempt {attempt + 1}: Database is not ready yet. Retrying in {delay_sec} seconds...")
                sleep(delay_sec)
            else:
                # For other exceptions, re-raise immediately
                raise
        raise Exception(f"Failed to connect to test database {TEST_DATABASE_URL} after {max_retries} attempts.")


def drop_test_db() -> None:
    """Drops the test database after terminating all active connections to it."""
    admin_engine = get_admin_engine()
    try:
        # Terminate all connections to the test database
        # Use AUTOCOMMIT isolation level to execute DROP DATABASE
        with admin_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            connection.execute(
                text(
                    f"""
                    SELECT pg_terminate_backend(pg_stat_activity.pid)
                    FROM pg_stat_activity
                    WHERE pg_stat_activity.datname = '{DB_NAME}_test'
                    AND pid <> pg_backend_pid();
                    """
                )
            )
            connection.execute(
                text(f"DROP DATABASE IF EXISTS {DB_NAME}_test;")
            )
    except ProgrammingError as e:
        print(f"Error dropping test database: {e}")
    finally:
        admin_engine.dispose()
