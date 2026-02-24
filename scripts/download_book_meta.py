import datetime
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event, BoundedSemaphore, Thread, Condition
from typing import List

from requests import Response
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from src.database.config import DATABASE_URL
from src.database.minio import get_minio_client
from src.logger import setup_logging
from src.models.record import IRecord, RecordStatus, StageInfo, EditionRecord
from src.pipeline.runner import SequentialOrchestrator
from src.pipeline.steps import PipelineStep
from src.pipeline.steps.actions import IADownloadManager
from src.repositories import EditionRepository

log = logging.getLogger("BookMetaDownload")

# --- Configuration ---
TIME_TO_REFILL_SECONDS = 60
MAX_PER_REFILL = 10  # Number of permits to add back to the limiter every refill interval
COOLDOWN_SECONDS = 1800  # 30 minutes
MAX_WORKERS = 2

# --- Global State ---
CV = Condition()
IS_PAUSED = False
STOP_PIPELINE = Event()
LIMITER = BoundedSemaphore(MAX_PER_REFILL)


def refill_limiter():
    """Refills 'MAX_PER_REFILL' permits every 60 seconds, but stays idle if paused."""
    while not STOP_PIPELINE.is_set():
        time.sleep(TIME_TO_REFILL_SECONDS)
        with CV:
            # If we are paused due to API limits, don't refill
            while IS_PAUSED:
                CV.wait()

            # Refill the semaphore
            log.info("Refilling rate limiter permits...")
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
        client = get_minio_client()

        try:
            # Ensure bucket exist if init_db was not run or bucket was deleted.
            if not client.bucket_exists(bucket_name):
                client.make_bucket(bucket_name)
        except Exception:
            pass

        response.raw.decode_content = True

        result = client.put_object(
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

        log.info(f"Successfully uploaded {file_name}. Etag: {result.etag}")
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
        Thread(target=refill_limiter, daemon=True).start()
        Thread(target=recovery_manager, daemon=True).start()

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for record in data:
                if STOP_PIPELINE.is_set():
                    log.info("Stop signal received. Halting new task submissions.")
                    break
                executor.submit(self.process_record, record)
            executor.shutdown(wait=False, cancel_futures=True)


# --- Execution ---

if __name__ == "__main__":
    setup_logging("book_download.log")
    engine = create_engine(DATABASE_URL, pool_size=MAX_WORKERS + 5)


    s = scoped_session(sessionmaker(bind=engine))
    repo = EditionRepository(session_factory=s)
    downloader = IADownloadManager(
        formats=['DjVuTXT'],
        callback=store_book_to_bucket
    )

    runner = RecoverableRunner(downloader, repo)
    data = repo.stream_all(batch_size=100)

    try:
        runner.run(data)
    except KeyboardInterrupt:
        trigger_stop()
        print("Shutting down...")
