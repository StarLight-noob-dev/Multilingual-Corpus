from typing import Any

from src.exceptions import PipelineError


class EmptyPipelineError(PipelineError):
    """Raised when no steps are configured in the pipeline."""
    def __init__(self, steps: Any):
        msg = f"Expected at least one step in the pipeline, but got {steps}"
        super().__init__(msg, step_name="Pipeline Initialization", payload={"steps": steps})


class StepExecutionError(PipelineError):
    """Raised when a step in the pipeline fails during execution."""
    def __init__(self, step_name: str, original_exception: Exception, payload: Any = None):
        msg = f"Error occurred in step '{step_name}': {str(original_exception)}"
        super().__init__(msg, step_name=step_name, payload=payload)


class InvalidStepError(PipelineError):
    """Raised when an invalid step is added to the pipeline."""
    def __init__(self, step_name:str, step: Any, payload: Any = None):
        msg = f"Invalid step provided, expected a PipelineStep instance but got {step}"
        super().__init__(msg, step_name=step_name, payload=payload)


class TestingLimitReached(PipelineError):
    """Raised when a predefined testing limit is reached during pipeline execution."""
    def __init__(self, step_name:str, limit: int):
        msg = f"Testing limit of {limit} reached."
        super().__init__(msg, step_name=step_name, payload={"limit": limit})