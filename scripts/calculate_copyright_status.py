import logging

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, scoped_session

from src.database.config import DATABASE_URL
from src.logger import setup_logging
from src.models.orm import EditionORM
from src.pipeline.error import ErrorPolicy, Action
from src.pipeline.runner import SequentialOrchestrator, BoundedScheduler
from src.pipeline.steps.actions import BufferedPostgresUpdater
from src.pipeline.steps.transformers.metadata.copyright import EditionCopyrightCalculation
from src.repositories import EditionRepository, AuthorRepository

log = logging.getLogger("calculate_copyright_status")


def main():
    setup_logging("calculate_copyright_status.log")

    max_workers = 1
    max_in_flight = max_workers * 1

    log.info("Starting calculate_copyright_status with %d workers (max in-flight=%d)", max_workers, max_in_flight)

    engine = create_engine(DATABASE_URL, pool_size=max_workers + 5)
    s = scoped_session(sessionmaker(bind=engine))
    error_policy = ErrorPolicy()

    edition_repo = EditionRepository(session_factory=s)
    author_repo = AuthorRepository(session_factory=s)

    # Pipeline: calculate copyright info using authors DB
    pipeline = SequentialOrchestrator(
        steps=[
            EditionCopyrightCalculation(repository=author_repo),
            (updater := BufferedPostgresUpdater(repository=edition_repo, buffer_size=10000))
        ],
        error_policy= error_policy
    )

    stmt = (
        select(EditionORM)
        .order_by(EditionORM.ol_id.asc())
    )

    data = edition_repo.stream_statement(stmt=stmt, batch_size=1)

    runner = BoundedScheduler(
        orchestrator=pipeline,
        repo=edition_repo,
        max_workers=70,
        max_per_refill=300,
        refill_seconds=1,
        cooldown_seconds=1
    )

    (error_policy
     .map_exception(KeyboardInterrupt, Action.STOP)
     .register_behavior(Action.STOP, lambda e, d: (log.info(f"Pipeline stopped due to: {e}"), runner.trigger_stop(e)))
     )

    try:
        runner.run(data)

    except KeyboardInterrupt as stop:
        log.info("Pipeline stopped by user: %s", stop)
        runner.trigger_stop(stop)

    except Exception as e:
        log.exception("Unhandled exception: %s", e)
        runner.trigger_stop(e)

    finally:
        log.info("Flushing remaining data to database...")
        updater.flush()


if __name__ == "__main__":
    main()
