import logging
import os
from pathlib import Path
from threading import Lock
from typing import Union

__all__ = ["root_logger", "get_logger"]

from src.logger.buffered_handler import BufferedFileHandler

# ---------- Logging helpers ----------
_logger_lock = Lock()

# Format to use for all handlers
FULL_FORMATTER = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s')

def root_logger(log_dir: Union[str, Path] = None,
                log_name: str = "pipeline.log",
                console_level: int = logging.DEBUG,
                erase_old_logs: bool = False,
                log_level: int = logging.NOTSET) -> logging.Logger:
    """
    Configure the root logger:

    - Console handler at console_level
    - Buffered file handler for the root file logging everything (NOTSET)
    - If erase_old_logs is True, delete all *.log files inside the resolved log_dir before setting up handlers.

    Default behaviour: if log_dir is None or a relative path is provided, the directory
    is resolved relative to the repository root (two parents up from this file), so
    the default becomes <project_root>/logs instead of src/logs.

    Args:
        log_dir (Union[str, Path], optional): Directory to store log files. Defaults to None.
        log_name (str, optional): Name of the root log file. Defaults to "pipeline.log".
        console_level (int, optional): Logging level for console output. Defaults to logging.DEBUG.
        erase_old_logs (bool, optional): If True, delete existing .log files in log_dir. Defaults to False.
        log_level (int, optional): Logging level for the root file handler. Defaults to logging.NOTSET.

    Returns the root logger.
    """
    # Resolve default log_dir to the project root logs directory, not src/logs
    if log_dir is None:
        # src/logger/__init__.py -> parents[2] should be the project root (Thesis)
        log_dir = Path(__file__).resolve().parents[2] / "logs"
    else:
        log_dir = Path(log_dir)
        # If a relative path was provided, resolve it relative to project root
        if not log_dir.is_absolute():
            log_dir = Path(__file__).resolve().parents[2] / log_dir

    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / log_name
    file_path_str = str(log_file.resolve())

    root = logging.getLogger()
    root.setLevel(log_level)

    # If requested, erase existing .log files in the resolved log_dir.
    if erase_old_logs:
        try:
            # First, safely close and remove any handlers that write into this directory
            for h in list(root.handlers):
                base = getattr(h, "baseFilename", None)
                if base:
                    try:
                        base_path = Path(base).resolve()
                        if base_path.parent == log_dir.resolve():
                            root.removeHandler(h)
                            try:
                                h.flush()
                            except Exception:
                                pass
                            try:
                                h.close()
                            except Exception:
                                pass
                    except Exception:
                        # ignore problems determining base filename
                        pass

            # Delete .log files
            for p in log_dir.glob("*.log"):
                try:
                    p.unlink()
                except Exception:
                    # best effort: don't raise during logger setup
                    pass
        except Exception:
            # swallow any unexpected errors during cleanup
            pass

    # Console handler (one only)
    with _logger_lock:
        stream_handlers = [h for h in root.handlers if isinstance(h, logging.StreamHandler)]
        if not stream_handlers:
            ch = logging.StreamHandler()
            ch.setLevel(console_level)
            ch.setFormatter(FULL_FORMATTER)
            root.addHandler(ch)
        else:
            for h in stream_handlers:
                h.setLevel(console_level)
                h.setFormatter(FULL_FORMATTER)

        # Root file handler: buffered
        existing_root_file = any(
            isinstance(h, (logging.FileHandler, BufferedFileHandler)) and getattr(h, "baseFilename", "") == file_path_str
            for h in root.handlers
        )
        if not existing_root_file:
            # Use BufferedFileHandler for root file
            root_file_handler = BufferedFileHandler(file_path_str, capacity=100, encoding="utf-8")
            root_file_handler.setLevel(log_level)
            root_file_handler.setFormatter(FULL_FORMATTER)
            root.addHandler(root_file_handler)

    return root


def get_logger(name: str,
               log_dir: str = None,
               propagate: bool = True,
               handler_level: int = logging.INFO,
               buffered: bool = False,
               buffer_capacity: int = 100) -> logging.Logger:
    """
    Get a module-specific logger.

    Important design choices:

    - The logger's level is set to NOTSET so that it DOES NOT filter records and
      allows propagation to the root logger (so the root file receives everything).
    - If a module-specific handler is requested, it will be added (if not already present).
      That handler will use `handler_level` to filter output for the module file.
    """

    if log_dir is None:
        log_dir = Path(__file__).resolve().parents[2] / "logs"
    else:
        log_dir = Path(log_dir)
        if not log_dir.is_absolute():
            log_dir = Path(__file__).resolve().parents[2] / log_dir

    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger(name)

    # Ensure thread-safe handler setup and avoid duplicate handlers for the same file
    module_log_path = os.path.join(log_dir, f"{name}.log")
    module_log_path_resolved = str(Path(module_log_path).resolve())

    with _logger_lock:
        # Add a module-specific handler only if requested and not already added
        if buffered:
            # check if a handler writing to this path already exists
            already = any(
                (isinstance(h, (logging.FileHandler, BufferedFileHandler)) and getattr(h, "baseFilename", "") == module_log_path_resolved)
                for h in logger.handlers
            )
            if not already:
                handler = BufferedFileHandler(module_log_path_resolved, capacity=buffer_capacity, encoding="utf-8")
                handler.setLevel(handler_level)
                handler.setFormatter(FULL_FORMATTER)
                logger.addHandler(handler)
        else:
            # if not buffered, add a plain FileHandler if not present
            already = any(
                (isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "") == module_log_path_resolved)
                for h in logger.handlers
            )
            if not already:
                fh = logging.FileHandler(module_log_path_resolved, encoding="utf-8")
                fh.setLevel(handler_level)
                fh.setFormatter(FULL_FORMATTER)
                logger.addHandler(fh)

    # Crucial: do NOT let the logger filter records before they propagate to root.
    # The one in charge of filtering is the handler(s).
    logger.setLevel(logging.NOTSET)
    logger.propagate = propagate

    return logger
