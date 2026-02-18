import logging
import time

from requests import Response

from src.config.paths import DataPaths
from src.database.minio import get_minio_client
from src.database.postgres import SessionLocal
from src.logger import setup_logging
from src.models.file_chunk import Chunk
from src.models.record import EditionRecord, RecordStatus
from src.pipeline.runner import SequentialOrchestrator
from src.pipeline.steps.actions import BufferedPostgresExporter
from src.pipeline.steps.transformers import EditionRecordParser
from src.pipeline.steps.transformers.metadata.copyright import EditionCopyrightCalculation
from src.reader.dump_reader import DumpReader
from src.repositories import EditionRepository, AuthorRepository

log = logging.getLogger("edition_meta_download")


def parse_data_and_upload(chunk: Chunk) -> None:
    start_time = time.time()

    edition_repo = EditionRepository(SessionLocal)
    author_repo = AuthorRepository(SessionLocal)
    db_exporter = BufferedPostgresExporter(repository=edition_repo, buffer_size=10000)

    pipeline = SequentialOrchestrator(
        steps=[
            EditionRecordParser(),
            EditionCopyrightCalculation(repository=author_repo),
            db_exporter # Uploads metadata to Postgres
        ]
    )

    records = DumpReader.record_generator(*chunk)
    for record in records:
        pipeline.run(record)

    end_time = time.time()
    log.info(f"Finished processing chunk {chunk} in {end_time - start_time:.2f} seconds.")


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

        log.info(f"Successfully uploaded {file_name}. Etag: {result.etag}")
        record.local_path = object_key
        record.status = RecordStatus.METADATA_EXTRACTED
        return record

    except Exception as e:
        log.exception(f"Failed to store {file_name} for ocaid={getattr(record, 'ocaid', None)}: {e}")
        record.local_path = None
        record.error = str(e)
        record.status = RecordStatus.ERROR
        return record


def cli():
    chunks = DumpReader.get_file_chunks(DataPaths.EDITION_DUMP, max_cpu=16)

    while True:
        print(f"There are a total of {len(chunks)} chunks to process:")
        for i, chunk in enumerate(chunks):
            print(f"\tChunk {i + 1}: Start={chunk.start}, End={chunk.end}, Size={chunk.end - chunk.start} bytes")
        print(f"(a for all, q to quit)")

        to_process = input("Which chunks would you like to process? (e.g., '1-3' or '1,3,5'): ")
        selected_chunks = []

        if to_process.strip() == "":
            print("\n"*10)
            print("No input provided. Please enter a valid option.\n")
            continue

        if to_process == "q":
            print("Exiting.")
            return

        elif to_process == "a":
            selected_chunks = chunks

        elif '-' in to_process:
            start, end = map(int, to_process.split('-'))
            if start < 0 or end < 0 or start > len(chunks) or end > len(chunks) or start > end:
                print("Invalid range. Please enter a valid range like '1-3'.")
                return
            selected_chunks = chunks[start - 1:end]
        else:
            indices = map(int, to_process.split(','))
            selected_chunks = [chunks[i - 1] for i in indices if 0 < i <= len(chunks)]

        if selected_chunks:
            break
        else:
            print("No valid chunks selected. Please try again.")

    for chunk in selected_chunks:
        print("\n")
        log.info(f"Started processing chunk: {chunk}")
        parse_data_and_upload(chunk)
        print("\n")


if __name__ == '__main__':
    setup_logging("process_editions_meta.log")
    cli()