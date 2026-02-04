from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from src.database.minio import get_minio_client, initialize_minio
from src.database.postgres import SessionLocal, initialize_postgres


def check_db_connection():
    """Checks if the database connection can be established."""
    try:
        session = SessionLocal()
        session.execute(text("SELECT 1"))
        session.close()
        print("PostgreSQL connection successful.")
        return True
    except OperationalError as e:
        print("PostgreSQL connection failed:", e)
        return False

def check_minio_connection():
    """Checks if the MinIO connection can be established."""
    try:
        client = get_minio_client()
        buckets = client.list_buckets()
        print("MinIO connection successful. Buckets:", [bucket.name for bucket in buckets])
        return True
    except Exception as e:
        print("MinIO connection failed:", e)
        return False

if __name__ == "__main__":
    if check_db_connection():
        import src.models.orm as orm # noqa: F401
        initialize_postgres()
        print("Database tables created successfully.")
    if check_minio_connection():
        initialize_minio()
