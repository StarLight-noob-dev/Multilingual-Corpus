from typing import Any

from src.exceptions import PipelineError


class EmptyPipelineError(PipelineError):
    """Raised when no steps are configured in the pipeline."""
    def __init__(self, steps: Any):
        msg = f"Expected at least one step in the pipeline, but got {steps}"
        super().__init__(msg, step_name="Pipeline Initialization", payload={"steps": steps})