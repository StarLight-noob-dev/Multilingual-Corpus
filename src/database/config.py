import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Load database config from environment (.env values or defaults) ---
DB_USER = os.getenv("DB_USER", "thesis_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "thesis_password")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "thesis_db")
DB_DRIVER = "postgresql"  # Explicitly define the driver

# --- Connection URLs ---
ADMIN_DATABASE_URL = f"{DB_DRIVER}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/postgres"
DATABASE_URL = f"{DB_DRIVER}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
TEST_DATABASE_URL = f"{DB_DRIVER}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}_test"


