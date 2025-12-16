import json
import os
import threading
from pathlib import Path
from typing import Dict, Any, Optional
from filelock import FileLock

from src.config.path_finder import ProjectRootFinder
from src.logger import get_logger
from src.models.results.stage_result import StageResult
from src.pipeline.context.context import PipelineContext
from src.pipeline.stage.interface import StageInterface

SUMMARY_JSON_FILE = "pipeline_stage_counting_summary.json"
LOCK_FILE = "pipeline_stage_counting_summary.lock"
LOG_SUBDIR = "logs/report/"


logger = get_logger(name="CountingStage")

class CountingStage(StageInterface):
    """
    A pipeline stage that counts processed records and aggregates counts across multiple threads/processes
    using a JSON file and file locking for safe concurrent access.
    Surely this can be done in a better way, but I can't with current multithreading/multiprocessing setup.

    Usage:

    1. Before starting the pipeline, call `CountingStage.reset_summary()` to clear any existing summary.
    2. Each instance of `CountingStage` should be initialized with a unique `usage_key`.
    3. During processing, the stage counts records in `process_batch`.
    4. On shutdown, it aggregates the local count into the shared JSON file using file locking.
    5. The final aggregate counts can be retrieved using `CountingStage.get_total_summary()`.
    """
    # Class-level cached file paths and lock
    _CACHED_JSON_PATH: Optional[Path] = None
    _CACHED_LOCK_PATH: Optional[Path] = None
    _CACHE_LOCK = threading.Lock()

    # Class-level constants for file names and directories
    _JSON_FILE = SUMMARY_JSON_FILE
    _LOCK_FILE = LOCK_FILE
    _LOG_DIR = LOG_SUBDIR

    def __init__(self, usage_key: str, stage_id: str = "", stage_name: str = "CountingStage"):
        super().__init__(stage_id=stage_id, stage_name=stage_name)
        self.usage_key = usage_key
        self._local_count = 0

    @staticmethod
    def _calculate_filepath(filename: str) -> Path:
        """Calculates and returns the full, absolute path to the file."""
        # Find the project root robustly
        start_dir = Path(__file__).resolve().parent
        root = ProjectRootFinder.find_project_root(start_dir)
        # Construct the path: <ROOT> / logs / report / filename
        target_dir = root / CountingStage._LOG_DIR
        # Ensure the directories exist before the first write
        target_dir.mkdir(parents=True, exist_ok=True)
        return target_dir / filename

    @classmethod
    def _get_json_filepath(cls) -> Path:
        """Returns the JSON file path, calculating it only on the first call."""
        if cls._CACHED_JSON_PATH is None:
            with cls._CACHE_LOCK:
                if cls._CACHED_JSON_PATH is None:
                    cls._CACHED_JSON_PATH = cls._calculate_filepath(cls._JSON_FILE)
        return cls._CACHED_JSON_PATH

    @classmethod
    def _get_lock_filepath(cls) -> Path:
        """Returns the lock file path, calculating it only on the first call."""
        if cls._CACHED_LOCK_PATH is None:
            with cls._CACHE_LOCK:
                if cls._CACHED_LOCK_PATH is None:
                    cls._CACHED_LOCK_PATH = cls._calculate_filepath(cls._LOCK_FILE)
        return cls._CACHED_LOCK_PATH

    @staticmethod
    def reset_summary() -> None:
        """
        Resets the summary by deleting the JSON and lock files.
        Called once in __main__ before starting the pipeline.
        """
        json_filepath = CountingStage._get_json_filepath()
        lock_filepath = CountingStage._get_lock_filepath()
        try:
            if os.path.exists(json_filepath):
                os.remove(json_filepath)
                print(f"Summary file reset: {json_filepath} deleted.")
            if os.path.exists(lock_filepath):
                os.remove(lock_filepath)
                print(f"Lock file reset: {lock_filepath} deleted.")
        except Exception as e:
            print(f"Error during file reset: {e}")

    @staticmethod
    def aggregate_local_count(usage_key: str, local_count: int) -> None:
        """
        Safely reads, updates, and writes the JSON file using a file lock.
        Called by shutdown() method in each thread/process.
        """
        lock = FileLock(CountingStage._get_lock_filepath())

        with lock:
            json_filepath = CountingStage._get_json_filepath()
            # Read current data (if file exists)
            if os.path.exists(json_filepath):
                with open(json_filepath, 'r') as f:
                    try:
                        data: Dict[str, int] = json.load(f)
                    except json.JSONDecodeError:
                        # Don't crash if the file is corrupted; start fresh
                        data = {}
            else:
                data = {}

            # Update the count
            current_total = data.get(usage_key, 0)
            data[usage_key] = current_total + local_count

            # Write data back to file
            with open(json_filepath, 'w') as f:
                json.dump(data, f, indent=4)

    @staticmethod
    def get_total_summary() -> Dict[str, Any]:
        """Reads and returns the final aggregate counts."""
        json_filepath = CountingStage._get_json_filepath()

        if os.path.exists(json_filepath):
            with open(json_filepath, 'r') as f:
                try:
                    return json.load(f)
                except json.JSONDecodeError:
                    return {"error": "Could not decode JSON summary."}
        return {}

    def initialize(self, stage_id: str, ctx: PipelineContext, **kwargs) -> Dict[str, Any]:
        self.stage_id = stage_id
        return {}

    def process_batch(self, stage_data: StageResult[Any, Any], ctx: PipelineContext, **kwargs) -> StageResult[Any, Any]:
        count = len(stage_data.success)
        self._local_count += count
        return stage_data

    def shutdown(self, ctx: PipelineContext) -> None:
        self.aggregate_local_count(self.usage_key , self._local_count)
        result = self.get_total_summary()
        final_count = result.get(self.usage_key, "N/A")
        print(f"[{threading.current_thread().name}] Stage {self.stage_id} ('{self.usage_key}') Summary:")
        print(f"  - Local Count (This Thread/Process): {self._local_count}")
        print(f"  - Final Global Total ('{self.usage_key}'): {final_count}")
