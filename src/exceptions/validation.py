from typing import Any, List

from .base_exception import PipelineError


class ValidationError(PipelineError):
    """Exception raised for validation errors in the pipeline."""

    def __init__(self, message: str, step_name:str = None, payload: Any = None, issues: List[str]=None):
        super().__init__(message, step_name, payload)
        self.issues = issues or []

    def to_dict(self):
        base_dict = super().to_dict()
        base_dict['issues'] = self.issues
        return base_dict

    def __str__(self):
        msg = super().__str__()
        if self.issues:
            return f"{msg} | Issues: {', '.join(self.issues)}"
        return msg