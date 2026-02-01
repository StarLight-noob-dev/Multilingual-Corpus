from concurrent.futures import ThreadPoolExecutor

from src.config.paths import DataPaths
from src.database.database import SessionLocal
from src.logger import clear_logs, setup_logging
from src.pipeline.steps.filters.lang_filters import LanguageFilter, AnyLanguageFilter
from src.reader.dump_reader import DumpReader
from src.pipeline.runner import SequentialOrchestrator
from src.pipeline.steps.aggregators import CountingAggregator
from src.pipeline.steps.transformers import EditionRecordParser
from src.pipeline.steps.filters.validation import (
    EditionsNecessaryFieldsFilter,
    FieldValidationFilter,
    EditionAnyCopyrightFilter
)
from src.pipeline.steps.utility.utility import EarlyPipelineStop
from src.pipeline.steps.actions import IADownloadManager
from src.repositories import AuthorRepository

MAX_WORKERS = 4


def demo() -> None:
    clear_logs()
    setup_logging()

    pipeline = SequentialOrchestrator(
        steps=[
            (n_entries := CountingAggregator()),
            EditionRecordParser(),
            EditionsNecessaryFieldsFilter(),
            AnyLanguageFilter(),  # Small demo dataset, so we don't filter by language
            #EditionCopyrightFilter(repository=AuthorRepository(SessionLocal())), # Requires a fully parsed author DB
            EditionAnyCopyrightFilter(),  # Just for demo purposes
            IADownloadManager(
                formats=['DjVuTXT'],
                extensions=['.txt'],
                base_dir=DataPaths.DOWNLOAD_DIR,
                delay=1.0
            ),
            FieldValidationFilter(
                validation_map={'local_path': lambda x: x is not None}
            ),
            EarlyPipelineStop(limit=5),
            (n_output := CountingAggregator()),
        ]
    )

    records = DumpReader.get_edition_sample_generator(size="big")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for record in records:
            executor.submit(pipeline.run, record)

    print(f"A total of {n_entries.count} entries were processed.")
    print(f"A total of {n_output.count} entries were outputted.")


if __name__ == '__main__':
    demo()
