import logging
import threading
from abc import ABC, abstractmethod
from typing import Any, Optional


logger = logging.getLogger(__name__) # TODO configure logger properly


class PipelineStep(ABC):
    """Abstract base class for all pipeline steps."""

    @abstractmethod
    def execute(self, data: Any) -> Optional[Any]:
        """Execute the pipeline step with the given data."""
        raise NotImplementedError()


class BaseFilter(PipelineStep):
    """Abstract base class for filter steps in the pipeline."""

    @abstractmethod
    def filter(self, data: Any) -> bool:
        """Determine if the data passes the filter criteria."""
        raise NotImplementedError()

    def execute(self, data: Any) -> Optional[Any]:
        try:
            return data if self.filter(data) else None
        except Exception as e:
            logger.error(f"Filter {self.__class__.__name__} failed: \n\t[*] Data: {data} \n\t[*] Error: {e}")
            return None


class BaseTransformer(PipelineStep):
    """Abstract base class for transformer steps in the pipeline."""

    @abstractmethod
    def transform(self, data: Any) -> Any:
        """Transform the input data and return the result."""
        raise NotImplementedError()

    def execute(self, data: Any) -> Optional[Any]:
        try:
            return self.transform(data)
        except Exception as e:
            logger.error(f"Transformer {self.__class__.__name__} failed: \n\t[*] Data: {data} \n\t[*] Error: {e}")
            return None


class BaseAction(PipelineStep):
    """Abstract base class for action steps in the pipeline."""

    @abstractmethod
    def perform(self, data: Any) -> None:
        """Perform an action using the input data."""
        raise NotImplementedError()

    def execute(self, data: Any) -> Optional[Any]:
        try:
            self.perform(data)
            return data
        except Exception as e:
            logger.error(f"Action {self.__class__.__name__} failed: \n\t[*] Data: {data} \n\t[*] Error: {e}")
            return None


class BaseAggregator(PipelineStep):
    """Abstract base class for aggregator steps in the pipeline."""
    def __init__(self):
        self._lock = threading.Lock()
        self.result = {}

    @abstractmethod
    def update(self, data: Any) -> Any:
        """Aggregate the input data and return the result."""
        raise NotImplementedError()

    def execute(self, data: Any) -> Optional[Any]:
        try:
            with self._lock:
                self.update(data)
            return data
        except Exception as e:
            logger.error(f"Aggregator {self.__class__.__name__} failed: \n\t[*] Data: {data} \n\t[*] Error: {e}")
            return None