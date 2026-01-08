from typing import List, Any

from src.exceptions import EmptyPipelineError
from src.pipeline.steps import PipelineStep


class SequentialOrchestrator:
    def __init__(self, steps: List[PipelineStep] = None):
        """
        Initialize the SequentialOrchestrator with a list of pipeline steps.

        Args:
            steps (List[PipelineStep]): A list of pipeline steps to be executed in sequence.

        Raises:
            EmptyPipelineError: If the steps list is None or empty.
        """
        if steps is None or len(steps) == 0:
            raise EmptyPipelineError(steps)
        self.steps = steps

    def run(self, record: Any) -> Any:
        current = record
        for step in self.steps:
            current = step.execute(current)
            if current is None:
                return None # Record was filtered out or an error occurred
        return current