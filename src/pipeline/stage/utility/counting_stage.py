from __future__ import annotations

import threading
from typing import Any, Dict, Iterable, Optional

from src.models.results.stage_result import StageResult
from src.pipeline.stage.interface import StageInterface
from src.pipeline.context.context import PipelineContext


class CountingStage(StageInterface):
    """A very small stage that counts how many elements it has seen.

    Behavior:
    - `process_batch` accepts either a `StageResult`, an iterable, or any object.
      If it's a `StageResult` we use its length. If it's an iterable we count its items
      (without materializing large iterators unnecessarily when possible).
    - The internal counter is thread-safe so multiple threads can call
      `process_batch` concurrently.
    - On `shutdown` the stage will print the total count to stdout. If a pipeline
      context exposes `io.write_shutdown_info` and `io.shutdown_file_path` those
      will be used to append the same summary to the provided file.
    """

    def __init__(self, stage_id: str = "", stage_name: str = "CountingStage"):
        super().__init__(stage_id=stage_id, stage_name=stage_name)
        self._count = 0
        self._lock = threading.Lock()

    def initialize(self, stage_id: str, ctx: PipelineContext, **kwargs) -> Dict[str, Any]:
        # allow caller to override the stage_name via kwargs
        name = kwargs.get("stage_name")
        if name:
            self.stage_name = name
        self.stage_id = stage_id
        return {"initialized": True, "stage_id": self.stage_id, "stage_name": self.stage_name}

    def process_batch(self, stage_data: Any, ctx: PipelineContext, **kwargs) -> Any:
        """Count items from various possible inputs.

        Returns the original stage_data unchanged so this stage can be used as a
        pass-through in pipelines.
        """
        n = 0
        # Fast path: StageResult defines __len__ and is intended for batches
        if isinstance(stage_data, StageResult):
            try:
                n = len(stage_data)
            except Exception:
                # fall back to iterating
                n = sum(1 for _ in stage_data.success_values())
        else:
            # If it's an iterable (and not a string/bytes), try to count efficiently
            if isinstance(stage_data, Iterable) and not isinstance(stage_data, (str, bytes)):
                try:
                    # Prefer using len() if available
                    n = len(stage_data)  # type: ignore[arg-type]
                except Exception:
                    # Fallback: iterate and count to support generators
                    count = 0
                    for _ in stage_data:  # type: ignore
                        count += 1
                    n = count
            else:
                # Single element
                n = 1

        # update the counter in a thread-safe manner
        with self._lock:
            self._count += int(n)

        return stage_data

    def shutdown(self, ctx: PipelineContext) -> None:
        summary = f"Stage '{self.stage_name}' processed total {self._count} items"
        # Print to stdout as requested
        print(summary)

        # If context provides IO flags, attempt to append the info to the shutdown file
        io = getattr(ctx, "io", None)
        write_flag = False
        file_path: Optional[str] = None
        if io is not None:
            write_flag = getattr(io, "write_shutdown_info", False)
            file_path = getattr(io, "shutdown_file_path", None)

        if write_flag and file_path:
            try:
                self.write_shutdown_info(file_path=file_path, text=summary)
            except Exception:
                # Best-effort: do not raise during shutdown
                pass

