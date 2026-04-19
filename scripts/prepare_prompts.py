import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Tuple, List, Dict, Set

import pandas as pd
from sqlalchemy import select, create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from tqdm import tqdm

from src.config.paths import DataPaths
from src.database.config import DATABASE_URL
from src.database.minio import get_minio_client
from src.logger import setup_logging
from src.models.orm import EditionORM
from src.models.record import EditionRecord
from src.repositories import EditionRepository, AuthorRepository

log = logging.getLogger("PreparePrompts")


MAX_WORKERS = 8
MAX_INFLIGHT = MAX_WORKERS * 10
BATCH_SIZE = 10000

def clean_text(text: str) -> str:
    lines = text.splitlines()
    lines = [line.strip() for line in lines if line.strip()]
    text = "\n".join(lines)
    text = re.sub(r'[ \t]+', ' ', text)
    return text.strip()


def extract_text_start_end(text: str,
                           first_chars: int = 10_000,
                           end_chars: int = 5_000) -> Optional[Tuple[str, str]]:
    if not text:
        return None

    c_text = clean_text(text)

    first_pages = c_text[:first_chars]
    end_pages = c_text[-end_chars:]
    return first_pages, end_pages


def write_batch(batch: List[Dict], output_path: str, file_index: int, use_parquet: bool) -> None:
    if not batch:
        return

    if use_parquet:
        df = pd.DataFrame(batch)
        out_file = os.path.join(output_path, f"prompt_data_{file_index}.parquet")
        df.to_parquet(out_file, engine="pyarrow", compression="snappy")
    else:
        out_file = os.path.join(output_path, f"prompt_data_{file_index}.jsonl")
        with open(out_file, "w", encoding="utf-8") as f:
            for row in batch:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")


def fetch_object_from_minio(client, bucket: str, object_name: str) -> Optional[str]:
    try:
        obj = client.get_object(bucket, object_name)
        try:
            raw = obj.read()
            if isinstance(raw, bytes):
                text = raw.decode("utf-8", errors="ignore")
            else:
                text = str(raw)
            return text
        finally:
            try:
                obj.close()
            except Exception:
                pass
            try:
                obj.release_conn()
            except Exception:
                pass
    except Exception as e:
        log.debug(f"MinIO fetch failed for {bucket}/{object_name}: {e}")
        return None


def process_edition(record: EditionRecord, minio_client, author_repo: AuthorRepository) -> Optional[dict]:
    try:
        file_uris = getattr(record, "file_uri", None) or []
        if not file_uris:
            return None

        text = None
        for uri in file_uris:
            text = fetch_object_from_minio(minio_client, "books", uri)
            if text:
                break

        if not text:
            return None

        boundaries = extract_text_start_end(text)
        if not boundaries:
            return None

        first_pages, end_pages = boundaries

        authors = [author_repo.get_author_name_by_id(author_id) for author_id in getattr(record, "authors", [])]
        title = getattr(record, "title", "")

        lang = "NA"
        if len(record.languages) == 1:
            lang = record.languages[0]
        if len(record.languages) > 1:
            lang = "multiple"

        return {
            "ID": getattr(record, "ol_id", ""),
            "title": title,
            "authors": ", ".join(authors),
            "language": lang,
            "first_pages": first_pages,
            "end_pages": end_pages,
        }

    except Exception as e:
        log.debug(f"Failed processing edition {getattr(record, 'ID', 'unknown')}: {e}")
        return None


def build_messages(title: Optional[str], authors: List[str], first_pages: str, end_pages: str) -> List[dict]:
    author_str = ", ".join(authors) if authors else ""
    if author_str:
        content_1 = f"What is the original date of publishing the Book: {title} by {author_str}"
    else:
        content_1 = f"What is the original date of publishing the Book: {title}"

    content_2 = f"The first pages of the book ({title}) are here: \n {first_pages}"
    content_3 = f"The last pages of the book ({title}) are here: \n {end_pages}"

    messages = [
        {"role": "system",
         "content": "You are a great accurate AI assistant that can figure out the original publishing year of any book, "
                    "you will get the title and author(s) of the book, and extracted text of the first and last pages of the book, and based on this information only, "
                    "you should figure out the year of publishing this book. If the book is a translation of another book, please consider the year "
                    "of publishing the translation book only. Please respond with only the year of publishing the book, "
                    "do not include any other information or texts. If you don't know the answer, please respond with 'no answer'."},
        {"role": "user", "content": content_1},
        {"role": "user", "content": content_2},
        {"role": "user", "content": content_3},
    ]
    return messages

def run(output_path: str,
         max_records: Optional[int] = None,
         use_parquet: bool = True) -> None:
    os.makedirs(output_path, exist_ok=True)

    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 60},
    )
    session_factory = scoped_session(sessionmaker(bind=engine))

    edition_repo = EditionRepository(session_factory=session_factory)
    author_repo = AuthorRepository(session_factory=session_factory)
    minio_client = get_minio_client()

    executor =ThreadPoolExecutor(max_workers=MAX_WORKERS)
    futures: List[Tuple] = []

    batch: List[Dict] = []
    processed_this_run: Set[str] = set()

    count = 0
    file_index = 0

    stmt = (select(EditionORM)
            .where(EditionORM.file_uri != '{}')
            .order_by(EditionORM.ol_id.asc())
            )

    for edition in tqdm(edition_repo.stream_statement(stmt), desc="Processing editions"):
        if max_records and count >= max_records:
            break

        if edition.ol_id in processed_this_run:
            continue

        future = executor.submit(process_edition, edition, minio_client, author_repo)
        futures.append((future, edition.ol_id))

        if len(futures) >= MAX_INFLIGHT:
            for future, fid in futures:
                result = future.result()
                if result:
                    batch.append(result)
                    processed_this_run.add(fid)

            futures.clear()

        if len(batch) >= BATCH_SIZE:
            write_batch(batch, output_path, file_index, use_parquet)
            batch.clear()
            processed_this_run.clear()
            file_index += 1

        count += 1
        if max_records and count >= max_records:
            break

    for future, fid in futures:
        result = future.result()
        if result:
            batch.append(result)
            processed_this_run.add(fid)

    if batch:
        write_batch(batch, output_path, file_index, use_parquet)

    executor.shutdown(wait=True)
    log.info(f"Done. Processed {count} editions")

if __name__ == '__main__':
    setup_logging("prepare_prompts.log")
    logging.basicConfig(level=logging.INFO)
    run(DataPaths.DATA_PROCESSED_DIR, max_records=None, use_parquet=True)

