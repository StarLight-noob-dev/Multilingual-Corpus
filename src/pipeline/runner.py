import logging
from typing import List, Any

from src.exceptions import EmptyPipelineError, InvalidStepError, TestingLimitReached
from src.pipeline.steps import PipelineStep

logger = logging.getLogger(__name__)


class SequentialOrchestrator:
    def __init__(self, steps: List[PipelineStep] = None):
        """
        Initialize the SequentialOrchestrator with a list of pipeline steps.

        Args:
            steps (List[PipelineStep]): A list of pipeline steps to be executed in sequence.

        Raises:
            EmptyPipelineError: If the steps list is None or empty.
        """
        if steps is None:
            raise EmptyPipelineError(steps)

        if not isinstance(steps, list):
            steps = list(steps)

        if len(steps) == 0:
            raise EmptyPipelineError(steps)

        for step in steps:
            if not isinstance(step, PipelineStep):
                raise InvalidStepError(
                    step_name="SequentialOrchestrator Initialization",
                    step=step.__class__,
                    payload={"expected_type": PipelineStep, "actual_type": type(step)}
                )

        self.steps = steps
        self._should_stop = False

    def run(self, record: Any) -> Any:
        if self._should_stop:
            return None

        current = record

        try:
            for step in self.steps:
                current = step.execute(current)
                if current is None:
                    return None # Record was filtered out or an error occurred
            return current
        except TestingLimitReached as e:
            logger.info(str(e))
            self._should_stop = True
            return None
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            return None