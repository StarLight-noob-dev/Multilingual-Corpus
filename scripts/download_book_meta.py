import datetime
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event, BoundedSemaphore, Thread, Condition
from typing import List

from requests import Response
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, scoped_session

from src.database.config import DATABASE_URL
from src.database.minio import get_minio_client
from src.logger import setup_logging
from src.models.orm import EditionORM
from src.models.record import IRecord, RecordStatus, StageInfo, EditionRecord
from src.pipeline.runner import SequentialOrchestrator
from src.pipeline.steps import PipelineStep
from src.pipeline.steps.actions import IADownloadManager
from src.repositories import EditionRepository

log = logging.getLogger("BookMetaDownload")

# --- Configuration ---
MAX_WORKERS = 70
IN_FLIGHT_MULTIPLIER = 10 # Submitted task per active thread
MAX_IN_FLIGHT = MAX_WORKERS * IN_FLIGHT_MULTIPLIER # Maximum number of outstanding submitted tasks (backpressure)
TIME_TO_REFILL_SECONDS = 1
MAX_PER_REFILL = 100
COOLDOWN_SECONDS = 1800  # 30 minutes

# --- Global State ---
CV = Condition()
IS_PAUSED = False
STOP_PIPELINE = Event()
LIMITER = BoundedSemaphore(MAX_PER_REFILL)
IN_FLIGHT = BoundedSemaphore(MAX_IN_FLIGHT)


def refill_limiter():
    """Refills 'MAX_PER_REFILL' permits every 60 seconds, but stays idle if paused."""
    while not STOP_PIPELINE.is_set():
        time.sleep(TIME_TO_REFILL_SECONDS)
        with CV:
            # If we are paused due to API limits, don't refill
            while IS_PAUSED:
                CV.wait()
            for _ in range(MAX_PER_REFILL):
                try:
                    LIMITER.release()
                except ValueError:
                    break


def recovery_manager():
    """Watches for the pause signal and manages the cooldown timer."""
    global IS_PAUSED
    while not STOP_PIPELINE.is_set():
        with CV:
            while not IS_PAUSED:
                CV.wait(timeout=1)
                if STOP_PIPELINE.is_set(): return
            log.warning(f"System Paused. Cooling down for {COOLDOWN_SECONDS}s...")
            time.sleep(COOLDOWN_SECONDS)
            log.debug("Recovery complete. Resuming pipeline...")
            IS_PAUSED = False
            CV.notify_all()


def trigger_pause():
    global IS_PAUSED
    with CV:
        if not IS_PAUSED:
            IS_PAUSED = True
            log.info("API Limit Triggered. Signaling all threads to pause.")


def trigger_stop():
    """Call this when a fatal error occurs (ValueError, RecursionError)."""
    if not STOP_PIPELINE.is_set():
        log.info("Fatal error detected. Flushing threads and shutting down...")
        STOP_PIPELINE.set()

        # 1. Wake up threads waiting on the Traffic Light (Pause/Recovery)
        with CV:
            CV.notify_all()

        # 2. Unblock threads waiting on the Rate Limiter
        # We release many permits to ensure everyone waiting at LIMITER.acquire() moves forward
        for _ in range(MAX_WORKERS * 2):
            try:
                LIMITER.release()
            except ValueError:
                break  # Semaphore is full, which is fine


mc_client = get_minio_client()


def store_book_to_bucket(file_name: str, response: Response, record: EditionRecord) -> EditionRecord:
    """
    Store streamed download (response) into MinIO 'books' bucket and set record.local_path.

    - Uploads response content under object key: <ocaid>/<file_name> (falls back to file_name if ocaid missing)
    - On failure sets record.local_path = None and populates record.error
    """
    bucket_name = "books"
    ol_id = record.ol_id
    if not ol_id:
        log.warning(f"No ol_id for file {file_name}. Using 'unknown' folder.")
        ol_id = "unknown"

    object_key = f"{ol_id}/{file_name}"
    content_length = int(response.headers.get('content-length', -1))
    content_type = response.headers.get('content-type', 'application/octet-stream')

    # Override content type for .txt files that are returned as generic octet-stream
    if file_name.endswith('.txt') and content_type == "application/octet-stream":
        content_type = "text/plain; charset=utf-8"

    log.info(f"Starting upload: {object_key}")
    log.debug(f"Metadata: Size={content_length} bytes, Type={content_type}, ocaid={getattr(record, 'ocaid', None)}")
    st = record.stages.get('book_download', None)

    try:

        if not mc_client.bucket_exists(bucket_name):
            mc_client.make_bucket(bucket_name)

        response.raw.decode_content = True

        result = mc_client.put_object(
            bucket_name=bucket_name,
            object_name=object_key,
            data=response.raw,
            length=content_length,
            content_type=content_type,
            part_size=10*1024*1024 if content_length == -1 else 0 # 10MB part size for multipart upload
        )

        record.file_uri.append(object_key)

        if st:
            msg = f" [{file_name}: Etag={result.etag} OK]"
            new_st = st.with_message(st.message + msg)
            record.stages['book_download'] = new_st

        log.debug(f"Successfully uploaded {file_name}. Etag: {result.etag}")
        return record

    except Exception as e:
        log.exception(f"Failed to store {file_name} for ocaid={getattr(record, 'ocaid', None)}: {e}")
        msg = f" [{file_name}: Error: {str(e)}]"
        if st:
            new_st = st.with_status(RecordStatus.ERROR).with_message(st.message + msg)
        else:
            new_st = StageInfo(status=RecordStatus.ERROR, message=msg, timestamp=str(datetime.datetime.now()))
        record.stages['book_download'] = new_st
        return record


class RecoverableRunner:
    def __init__(self, orchestrator: SequentialOrchestrator | List[PipelineStep] | PipelineStep, repo: EditionRepository):
        self.orchestrator = orchestrator
        self.repository = repo

    def _wait_if_paused(self):
        """Standard check for threads to halt at the traffic light."""
        with CV:
            while IS_PAUSED:
                CV.wait()

    def process_record(self, record: IRecord):
        global IS_PAUSED

        # Check if we are paused by the recovery manager
        self._wait_if_paused()

        # Acquire rate-limit permit (Wait here if exhausted)
        LIMITER.acquire()

        # Check pause again (in case it was paused while we were waiting for permit)
        self._wait_if_paused()

        if STOP_PIPELINE.is_set():
            return

        log.info("Processing record: %s", getattr(record, 'ol_id', str(record)))

        try:
            if isinstance(self.orchestrator, SequentialOrchestrator):
                r = self.orchestrator.run(record)
                if r:
                    self.repository.update_entity(r)

            elif isinstance(self.orchestrator, list):
                if all(isinstance(step, PipelineStep) for step in self.orchestrator):
                    current = record
                    for step in self.orchestrator:
                        current = step.execute(current)
                        if current is None:
                            break
                    if current:
                        self.repository.update_entity(current)

            elif isinstance(self.orchestrator, PipelineStep):
                r = self.orchestrator.execute(record)
                if r:
                    self.repository.update_entity(r)

            else:
                raise ValueError("Invalid orchestrator type provided.")

        except ValueError:
            log.exception(f"Failed to execute {record}")
            trigger_stop()

        except RecursionError as re:
            log.error(f"RecursionError on {record}. Skipping record. {re.__traceback__}")
            trigger_stop()

        except Exception as e:
            if "429" in str(e) or "limit" in str(e).lower() or "throttle" in str(e).lower():
                log.warning(f"API limit hit for {record}. Triggering pause. Error: {e}")
                trigger_pause()
            else:
                log.error(f"Worker Error on {record}: {e}")

    def run(self, data):
        """Streams IDs from the database to keep memory usage low."""
        # Start helper threads
        total = 0

        Thread(target=refill_limiter, daemon=True).start()
        Thread(target=recovery_manager, daemon=True).start()

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

            for record in data:
                if STOP_PIPELINE.is_set():
                    log.info("Stop signal received. Halting new task submissions.")
                    break

                # Backpressure: block if too many outstanding tasks are pending
                IN_FLIGHT.acquire()
                total += 1
                if total % 10000 == 0:
                    log.info(f"Submitted {total} records for processing. Current in-flight tasks: {MAX_IN_FLIGHT - IN_FLIGHT._value}")
                executor.submit(self._task_wrapper, record)

            # Wait for all submitted tasks to finish before exiting.
            # This prevents the producer (DB stream) from outrunning the executor and building an unbounded queue.
            executor.shutdown(wait=True)

    def _task_wrapper(self, rec: IRecord):
        try:
            self.process_record(rec)
        finally:
            try:
                IN_FLIGHT.release()
            except Exception:
                log.debug("IN_FLIGHT.release() failed or semaphore already full")


if __name__ == "__main__":
    clear_logs()
    setup_logging("book_download.log")
    engine = create_engine(DATABASE_URL, pool_size=MAX_WORKERS + 5)

    s = scoped_session(sessionmaker(bind=engine))
    repo = EditionRepository(session_factory=s)
    downloader = IADownloadManager(
        formats=['DjVuTXT'],
        callback=store_book_to_bucket,
        delay=1.0
    )

    stmt = (
        select(EditionORM)
        .where(
            EditionORM.ocaid.is_not(None),
            EditionORM.ocaid != "",
            EditionORM.stages == '{}' #type: ignore
        )
    )

    data = repo.stream_statement(stmt=stmt, batch_size=500)

    runner = RecoverableRunner(downloader, repo)

    try:
        runner.run(data)

    except KeyboardInterrupt:
        print("Shutting down for KeyboardInterrupt...")
        STOP_PIPELINE.set()
        trigger_stop()

    except Exception as e:
        log.exception(f"Unhandled exception: {e}")
        trigger_stop()