from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

from src.models.results.stage_result import StageResult

import threading
from pathlib import Path

from src.pipeline.stage.context import PipelineContext

# Map output path -> threading.Lock to coordinate writes from multiple threads.
_file_locks: Dict[str, threading.Lock] = {}
# Protects access to the _file_locks mapping itself.
_file_locks_lock = threading.Lock()


def _get_lock_for_path(path: str) -> threading.Lock:
    """Return a Lock object for the given path, creating one if necessary."""
    with _file_locks_lock:
        lock = _file_locks.get(path)
        if lock is None:
            lock = threading.Lock()
            _file_locks[path] = lock
        return lock


class StageInterface(ABC):

    def __init__(self, stage_id: str = "", stage_name: str = ""):
        self.stage_id = stage_id
        self.stage_name = stage_name

    @abstractmethod
    def initialize(self, stage_id: str, ctx: PipelineContext, **kwargs) -> Dict[str, Any]:
        """
        Initialize the stage with the given ID, optional parameters can be passed via kwargs for
        stage-specific configuration.

        Args:
            stage_id (str): Unique identifier for the stage.
            ctx (PipelineContext): The pipeline context containing stage specific information.
            **kwargs: Optional parameters for stage-specific configuration.

        Returns:
            dict: A dictionary containing initialization results or configuration details.
        """
        raise NotImplementedError("This method should be overridden by subclasses.")

    @abstractmethod
    def process_batch(self, stage_data: StageResult[Any, Any], ctx: PipelineContext,**kwargs) -> StageResult | Any | None: #TODO type stage result
        """
        This method defines how the stage processes a batch of data.

        Args:
            stage_data: The batch of data to be processed. Either an iterable of records or a StageResult encapsulating
            all prior processing results.
            **kwargs: Optional parameters for stage-specific processing.

        Returns:
            An `StageResult` instance summarizing the results of processing the batch such as
            success/failure counts and details or None if the stage is a consumer in which case
            the stage must declare it is a consumer.
        """
        raise NotImplementedError("This method should be overridden by subclasses.")

    @abstractmethod
    def shutdown(self, ctx: PipelineContext) -> None:
        """
        Perform any necessary cleanup before shutting down the stage.

        New optional parameters allow callers to request that a textual summary or other
        information be appended to a file in a thread-safe manner:

        Args:
            write_info (bool): If True the stage should write `info` to `file_path` as part of shutdown.
            file_path (Optional[str]): Path to append the shutdown information to. If None, implementations
                                     may choose a sensible default.
            info (Optional[str]): Text to write. If None, implementations may serialize internal
                                  summary state (for example the StageResult summary).
        """
        raise NotImplementedError("This method should be overridden by subclasses.")

    def write_shutdown_info(self, file_path: Optional[str] = None, text: Optional[str] = None) -> None:
        """
        Thread-safe helper that appends `text` to `file_path`. If `file_path` is None this will raise
        a ValueError. This method uses a per-path threading.Lock so multiple threads can append
        to the same file safely.
        """
        if file_path is None:
            raise ValueError("file_path must be provided to write shutdown info")

        # Normalize path
        p = Path(file_path).expanduser().resolve()
        p.parent.mkdir(parents=True, exist_ok=True)

        lock = _get_lock_for_path(str(p))
        with lock:
            with open(p, "a", encoding="utf-8") as fh:
                if text is None:
                    text = ""
                fh.write(text)
                fh.write("\n")
