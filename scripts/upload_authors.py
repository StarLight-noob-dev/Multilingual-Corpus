from concurrent.futures import ThreadPoolExecutor

from tqdm import tqdm

from src.database.database import SessionLocal
from src.logger import clear_logs, setup_logging
from src.pipeline.runner import SequentialOrchestrator
from src.pipeline.steps.actions import BufferedPostgresExporter
from src.pipeline.steps.transformers import AuthorRecordParser
from src.reader.dump_reader import DumpReader
from src.repositories import AuthorRepository


def upload_authors():
    MAX_WORKERS = 16
    repo = AuthorRepository(SessionLocal)
    pipeline = SequentialOrchestrator(
        steps=[
            AuthorRecordParser(),
            (db_exporter := BufferedPostgresExporter(repository=repo))
        ]
    )

    records = DumpReader.get_author_generator()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for record in tqdm(records, unit=" recs", desc="Uploading authors"):
            executor.submit(pipeline.run, record)

    db_exporter.flush()

    # 14751564 From OL dump
    print(f"There are now {repo.count()} authors in the database.")


if __name__ == '__main__':
    clear_logs()
    setup_logging()
    upload_authors()

