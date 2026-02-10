import logging
import time
from concurrent.futures import ThreadPoolExecutor
from time import sleep

from tqdm import tqdm

from src.config.paths import DataPaths
from src.database.postgres import SessionLocal
from src.logger import clear_logs, setup_logging
from src.models.file_chunk import Chunk
from src.pipeline.runner import SequentialOrchestrator
from src.pipeline.steps.actions import BufferedPostgresExporter
from src.pipeline.steps.transformers import AuthorRecordParser
from src.reader.dump_reader import DumpReader
from src.repositories import AuthorRepository

log = logging.getLogger("author upload")

def upload_authors(chunk: Chunk):
    MAX_WORKERS = 2

    start_time = time.time()
    log.info(f"Started upload_authors() at {start_time}")

    repo = AuthorRepository(SessionLocal)
    pipeline = SequentialOrchestrator(
        steps=[
            AuthorRecordParser(),
            (db_exporter := BufferedPostgresExporter(repository=repo, buffer_size=10000)),
        ]
    )

    records = DumpReader.record_generator(*chunk)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for record in tqdm(records, unit=" recs", desc="Pushing author records to pipeline"):
            executor.submit(pipeline.run, record)

    db_exporter.flush()

    # 14751564 From OL dump
    end_time = time.time()
    log.debug(
        f"Uploaded authors to database using {MAX_WORKERS} workers."
        f"- Total time: {end_time - start_time} seconds."
        f"- DB has {repo.count()} authors after upload. Expected ~14.75 million."
    )
    log.info(f"Finished upload_authors() in {end_time - start_time} seconds.")


def cli():
    chunks = DumpReader.get_file_chunks(DataPaths.AUTHOR_DUMP, max_cpu=16)

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
        upload_authors(chunk)


if __name__ == '__main__':
    clear_logs()
    setup_logging()
    cli()
