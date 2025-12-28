from typing import Any, List

from .base_exception import PipelineError


class ValidationError(PipelineError):
    """Exception raised for validation errors in the pipeline."""

    def __init__(self, message: str, step_name:str = None, payload: Any = None, issues: List[str]=None):
        super().__init__(message, step_name, payload)
        self.issues = issues or []

    def __str__(self):
        msg = super().__str__()
        if self.issues:
            return f"{msg} | Issues: \n\t[*] {'\n\t[*] '.join(self.issues)}"
        return msg