import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Load database config from environment (.env values or defaults) ---
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "thesis_db"),
    "user": os.getenv("DB_USER", "thesis_user"),
    "password": os.getenv("DB_PASSWORD", "thesis_password"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "driver": os.getenv("DB_DRIVER", "postgresql")
}

# --- Load minio config from environment ---
MINIO_CONFIG = {
    "endpoint": os.getenv("MINIO_ENDPOINT", "localhost:9000"),
    "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    "secure": os.getenv("MINIO_SECURE", "false").lower() == "true",
    "max_connections": int(os.getenv("MINIO_MAX_CONNECTIONS", "10")),
    "retries": int(os.getenv("MINIO_RETRIES", 3)),
    "timeout": int(os.getenv("MINIO_TIMEOUT", 60))
}

# --- Connection URLs ---
ADMIN_DATABASE_URL = f"{DB_CONFIG["driver"]}://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/postgres"
DATABASE_URL = f"{DB_CONFIG["driver"]}://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["dbname"]}"
TEST_DATABASE_URL = f"{DB_CONFIG["driver"]}://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["dbname"]}_test"


