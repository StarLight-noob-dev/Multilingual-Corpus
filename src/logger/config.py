import logging
import shutil
from logging.handlers import RotatingFileHandler

from src.config.paths import DataPaths
from src.logger.buffered_handler import BufferedFileHandler


def setup_logging(file_name: str = "pipeline.log") -> None:
    # Ensure log directory exists
    log_dir = DataPaths.LOGS_DIR
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / file_name

    # Define the Formatter
    formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s')

    # Setup Console Handler
    console_h = logging.StreamHandler()
    console_h.setLevel(logging.INFO)
    console_h.setFormatter(formatter)

    # Setup Main Buffered File Handler
    file_h = RotatingFileHandler(
        log_path,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding='utf-8'
    )
    file_h.setLevel(logging.DEBUG)
    file_h.setFormatter(formatter)

    # Configure Root Logger
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(console_h)
    root.addHandler(file_h)

def clear_logs():
    YELLOW = "\033[93m"
    RESET = "\033[0m"
    BOLD = "\033[1m"

    log_dir = DataPaths.LOGS_DIR

    if log_dir.exists():
        for item in log_dir.iterdir():
            try:
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    shutil.rmtree(item)
            except Exception as e:
                print(f"{YELLOW}{BOLD}WARNING:{RESET} {YELLOW}Could not delete log file {item.name}. "
                      f"It may be in use by another process. Error: {e}{RESET}"
                      )