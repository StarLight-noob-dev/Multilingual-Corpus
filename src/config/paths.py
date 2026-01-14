import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Define root directory two levels up from this file
ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[2]))

class DataPaths:
    # Define important subdirectories
    DATA_DIR = ROOT / "data"
    DATA_RAW_DIR = DATA_DIR / "raw"
    DATA_SAMPLES_DIR = DATA_DIR / "samples"
    DOWNLOAD_DIR = DATA_DIR / "downloads"
    LOGS_DIR = ROOT / "logs"

    # Path to data files
    EDITION_DUMP = DATA_RAW_DIR / 'ol_dump_editions_latest.txt'
    AUTHOR_DUMP = DATA_RAW_DIR / 'ol_dump_authors_latest.txt'

    BIG_EDITION_SAMPLE = DATA_SAMPLES_DIR / 'big_editions_sample.txt'
    MEDIUM_EDITION_SAMPLE = DATA_SAMPLES_DIR / 'medium_editions_sample.txt'
    SMALL_EDITION_SAMPLE = DATA_SAMPLES_DIR / 'small_editions_sample.txt'

    BIG_AUTHOR_SAMPLE = DATA_SAMPLES_DIR / 'big_authors_sample.txt'
    MEDIUM_AUTHOR_SAMPLE = DATA_SAMPLES_DIR / 'medium_authors_sample.txt'
    SMALL_AUTHOR_SAMPLE = DATA_SAMPLES_DIR / 'small_authors_sample.txt'