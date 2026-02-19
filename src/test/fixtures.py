from typing import Iterator

import pytest
from sqlalchemy.orm import Session, sessionmaker, scoped_session

from src.database.base import Base
from src.database.postgres import get_test_engine, drop_test_db


@pytest.fixture(scope="session")
def engine():
    """
    Fixture to create the test database and all tables at the start of the test session.

    Teardown drops the entire test database.
    """
    # SETUP: Create the database (if needed) and get the engine
    test_engine = get_test_engine()

    # SETUP: Create all tables
    Base.metadata.create_all(bind=test_engine)

    yield test_engine  # Execution passes to the tests

    # TEARDOWN: Drop the test database entirely
    test_engine.dispose()
    drop_test_db()


@pytest.fixture(scope="function")
def session(engine) -> Iterator[scoped_session]:
    """
    Provides a transactional session factory, ensuring each test is isolated.
    Changes are rolled back after the test completes.
    """
    # Start a connection and transaction
    connection = engine.connect()
    transaction = connection.begin()

    # Bind the sessionmaker to the connection and wrap in scoped_session
    SessionFactory = sessionmaker(bind=connection)
    session_factory = scoped_session(SessionFactory)

    yield session_factory  # Execution passes to the test function

    # TEARDOWN: Rollback the transaction to reset state
    transaction.rollback()
    connection.close()