import logging
from sqlalchemy.exc import OperationalError

from src.database.database import init_db, SessionLocal


def check_db_connection():
    """Checks if the database connection can be established."""
    try:
        session = SessionLocal()
        session.execute("SELECT 1")
        session.close()
        logging.info("Main Database connection successful.")
        return True
    except OperationalError as e:
        print("Main Database connection failed:", e)
        return False

if __name__ == "__main__":
    if check_db_connection():
        print("Creating database tables...")
        init_db()
        print("Database tables created successfully.")

