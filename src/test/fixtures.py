from typing import Iterator

import pytest
from sqlalchemy.orm import Session

from src.database.base import Base
from src.database.database import get_test_engine, drop_test_db


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
def session(engine) -> Iterator[Session]:
    """
    Provides a transactional session, ensuring each test is isolated.
    Changes are rolled back after the test completes.
    """
    # Start a connection and transaction
    connection = engine.connect()
    transaction = connection.begin()

    # Bind the session to the connection
    session = Session(bind=connection)

    yield session  # Execution passes to the test function

    # TEARDOWN: Rollback the transaction to reset state
    session.close()
    transaction.rollback()
    connection.close()