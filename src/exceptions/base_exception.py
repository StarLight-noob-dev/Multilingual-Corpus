from typing import Any


class PipelineError(Exception):
    """Base class for all pipeline-related exceptions."""
    def __init__(self, message: str, step_name: str = None, payload: Any = None):
        super().__init__(message)
        self.step_name = step_name
        self.payload = payload

    def to_dict(self):
        return {
            "error_type": self.__class__.__name__,
            "step": self.step_name,
            "message": str(self),
            "payload": self.payload,
        }