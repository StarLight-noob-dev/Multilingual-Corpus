import logging
import threading
import time
from concurrent.futures.thread import ThreadPoolExecutor
from typing import List, Any, override

from src.exceptions import EmptyPipelineError, InvalidStepError
from src.pipeline.error import ErrorPolicy, Action
from src.pipeline.steps import PipelineStep
from src.repositories import BaseSqlRepository

logger = logging.getLogger(__name__)


class Orchestrator:
    def __init__(self, error_policy: ErrorPolicy = None, max_retries: int = 3):
        self.policy = error_policy
        self.max_retries = max_retries

    def run(self, record: Any) -> Any:
        raise NotImplementedError("Orchestrator subclasses must implement the run method.")

    def _default_fallback(self, e: Exception, action: Action, record: Any) -> None:
        logger.error(f"Unhandled exception: {e} for record: {record}")
        raise e


class SequentialOrchestrator(Orchestrator):
    def __init__(self, steps: List[PipelineStep] = None, error_policy: ErrorPolicy = None, max_retries: int = 3):
        """
        Initialize the SequentialOrchestrator with a list of pipeline steps and an optional error policy.

        Args:
            steps (List[PipelineStep]): A list of pipeline steps to be executed in sequence.
            error_policy (ErrorPolicy): An optional error policy to determine how to handle exceptions during
                step execution.
            max_retries (int): The maximum number of retries for steps that fail with a RETRY action.

        Raises:
            EmptyPipelineError: If the steps list is None or empty.
        """
        self.max_retries = None
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
        super().__init__(error_policy, max_retries)

    @override
    def run(self, record: Any) -> Any:
        current = record
        for step in self.steps:
            attempts = 0
            while attempts < self.max_retries:
                try:
                    current = step.execute(current)
                    if current is None: return None # No success
                    break # Success, move to next step
                except Exception as e:
                    action = self.policy.get_action(e)

                    if action is Action.RETRY and attempts < self.max_retries:
                        attempts += 1
                        logger.warning(f"Retry {attempts}/{self.max_retries} for {step.__class__.__name__}")
                        time.sleep(1 * attempts)
                        continue

                    self._handle_recovery(e, record)
                    return None
        return current

    def _handle_recovery(self, e: Exception, record: Any) -> None:
        action, behavior = self.policy.get_recovery_details(e)
        if behavior:
            behavior(e, record)
        else:
            self._default_fallback(e, action, record)


class BoundedScheduler:
    def __init__(
            self,
            orchestrator: SequentialOrchestrator,
            repo: BaseSqlRepository,
            max_workers: int = 8,
            max_per_refill: int = 100,
            refill_seconds: int = 60,
            cooldown_seconds: int = 1800
    ):
        self.orchestrator = orchestrator
        self.repo = repo
        self.log = logging.getLogger(self.__class__.__name__)

        # --- Internal State ---
        self.cv = threading.Condition()
        self.is_paused = False
        self.stop_event = threading.Event()

        # --- Internal Limits ---
        self.limiter = threading.BoundedSemaphore(max_per_refill)
        self.in_flight = threading.BoundedSemaphore(max_workers * 10)

        # --- Settings ---
        self.max_workers = max_workers
        self.max_per_refill = max_per_refill
        self.refill_seconds = refill_seconds
        self.cooldown_seconds = cooldown_seconds

    # --- Internal Management Logic ---

    def _refill_limiter_loop(self):
        """Refills the limiter permits periodically."""
        while not self.stop_event.is_set():
            time.sleep(self.refill_seconds)
            with self.cv:
                while self.is_paused and not self.stop_event.is_set():
                    self.cv.wait()

                for _ in range(self.max_per_refill):
                    try:
                        self.limiter.release()
                    except ValueError:
                        break

    def _recovery_manager_loop(self):
        """Manages the cooldown timer when paused."""
        while not self.stop_event.is_set():
            with self.cv:
                while not self.is_paused and not self.stop_event.is_set():
                    self.cv.wait(timeout=1)

                if self.stop_event.is_set(): break

                self.log.warning(f"Cooldown active: {self.cooldown_seconds}s...")

            time.sleep(self.cooldown_seconds)

            with self.cv:
                self.is_paused = False
                self.log.info("Cooldown finished. Resuming...")
                self.cv.notify_all()

    def trigger_pause(self, e=None, record=None):
        with self.cv:
            if not self.is_paused:
                self.is_paused = True
                self.log.warning(f"Pause triggered by {type(e).__name__}")

    def trigger_stop(self, e=None, record=None):
        if not self.stop_event.is_set():
            self.log.critical("Stop signal received.")
            self.stop_event.set()
            with self.cv:
                self.cv.notify_all()
            # Flush the limiter so waiting threads can exit
            for _ in range(self.max_workers * 2):
                try:
                    self.limiter.release()
                except ValueError:
                    break

    def _task_wrapper(self, record):
        try:
            with self.cv:
                while self.is_paused and not self.stop_event.is_set():
                    self.cv.wait()

            self.limiter.acquire()

            if self.stop_event.is_set(): return

            result = self.orchestrator.run(record)
            if result:
                self.repo.update_entity(result)
        finally:
            try:
                self.in_flight.release()
            except ValueError:
                pass

    def run(self, data_stream):
        # Start manager threads as daemons tied to this instance
        threading.Thread(target=self._refill_limiter_loop, daemon=True).start()
        threading.Thread(target=self._recovery_manager_loop, daemon=True).start()

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for record in data_stream:
                if self.stop_event.is_set():
                    break

                self.log.debug("Processing record: %s", getattr(record, 'ol_id', str(record)))
                self.in_flight.acquire()
                executor.submit(self._task_wrapper, record)

            executor.shutdown(wait=True)