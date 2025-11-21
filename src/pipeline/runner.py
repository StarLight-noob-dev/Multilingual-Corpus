import logging
import multiprocessing as mp
from threading import current_thread #This might be incorrect but to the best of my knowledge im working with threads.
from collections.abc import Callable
from typing import List, Tuple

from src.models.file_chunk import Chunk
from src.models.record.record_factory import process_record
from src.models.record.transport_record import TransportRecord
from src.models.results.stage_result import StageResult
from src.reader.dump_reader import DumpReader
from src.pipeline.stage.context import PipelineContext
from src.pipeline.stage.interface import StageInterface

logger = logging.getLogger(__name__)


def worker_thread(file_name: str,
                  chunk_start: int,
                  chunk_end: int,
                  batch_size: int = 250,
                  thread_id: str = None,
                  stage_factory: List[Callable[[], StageInterface]] = None,
                  ctx_factory: List[Callable[[], PipelineContext]] = None
                  ) -> None:
    """
    Worker thread function to process a chunk of data through a pipeline of stages.

    Is up to the user to set the right stages and context depending on the data being processed.

    This function utilizes its own instance of `DumpReader` to build the generator for the given file;
    Then initializes the pipeline stages, reads records from the specified chunk, processes them
    in batches through the pipeline, and finally shuts down the stages.

    It will create an entry stage result from the batch containing TransportRecords. This means that
    is not necessary to have a specific stage to read the records, but the user must ensure that the
    following stage can handle TransportRecords as input and that contracts from the stages after
    that are satisfied.

    Args:
        file_name (str): The name of the file to read data from.
        chunk_start (int): The starting byte position of the chunk to process.
        chunk_end (int): The ending byte position of the chunk to process.
        batch_size (int): The size of each batch to process.
        thread_id (str): Optional identifier for the thread.
        stage_factory (List[Callable[[], StageInterface]]): List of callables that produce StageInterface instances.
        ctx_factory (List[Callable[[], PipelineContext]]): List of callables that produce PipelineContext instances.
    """
    t_name = thread_id or current_thread().name

    if stage_factory is None:
        logger.info(f"No stages where given, no processing will be done in thread {t_name}")

    if ctx_factory is None:
        logger.info(f"No context factory where given, no processing will be done in thread {t_name}")

    # Initialize pipeline stages before processing and validate factories
    pipeline = _initialize_stages(stage_factory, ctx_factory)

    # Read records from the specified chunk, is up to the user to set the right stages and context
    # depending on the data being processed
    dr = DumpReader()
    gen = dr.record_from_chunk_gen(file_name, chunk_start, chunk_end)

    for batch in dr.batch_generator(gen, batch_size):
        # Create initial stage result from the batch containing transport records to start the pipeline
        current = _make_entry_stage_from_generator(batch)
        # Process the batch through the pipeline
        for stage, ctx in pipeline:
            result = stage.process_batch(current, ctx)
            current = result

    logger.info(f"Thread {t_name} finished processing chunk {chunk_start}-{chunk_end}")

    # Shutdown all stages after processing, in reverse order.
    for stage, ctx in reversed(pipeline):
        try:
            stage.shutdown(ctx)  # Not all stages need shutdown, but those that do will implement it.
            logger.debug(f"Shutdown stage {stage.stage_name} in thread {t_name}")
        except Exception as e:
            logger.exception(f"Error during shutdown of stage {stage.stage_name} in thread {t_name}: {e}")


def process_chunks_in_pool(stage_factory: List[Callable[[], StageInterface]],
                   ctx_factory: List[Callable[[], PipelineContext]],
                   data_chunks: List[Chunk],
                   num_threads: int = 4) -> None:
    """
    Process multiple data chunks in parallel using a pool of worker threads. Calling `worker_thread`
    for each chunk.

    Args:
        stage_factory (List[Callable[[], StageInterface]]): List of callables that produce StageInterface instances.
        ctx_factory (List[Callable[[], PipelineContext]]): List of callables that produce PipelineContext instances.
        data_chunks (List[Chunk]): List of data chunks to process.
        num_threads (int): Number of threads to use in the pool.
    """
    with mp.Pool(num_threads) as p:
        p.starmap(
            worker_thread,
            [
                (chunk.file_name, chunk.start, chunk.end, 250, f"Thread-{i}", stage_factory, ctx_factory)
                for i, chunk in enumerate(data_chunks)
            ]
        )

def process_chunk(
    stage_factory: List[Callable[[], StageInterface]],
    ctx_factory: List[Callable[[], PipelineContext]],
    chunk: Chunk,
    batch_size: int = 250,
    thread_id: str = None
) -> None:
    """
    Process a single data chunk using the worker_thread function.

    Args:
        stage_factory (List[Callable[[], StageInterface]]): List of callables that produce StageInterface instances.
        ctx_factory (List[Callable[[], PipelineContext]]): List of callables that produce PipelineContext instances.
        chunk (Chunk): The data chunk to process.
        batch_size (int): The size of each batch to process.
        thread_id (str): Optional identifier for the thread.
    """
    return worker_thread(
        file_name=chunk.file_name,
        chunk_start=chunk.start,
        chunk_end=chunk.end,
        batch_size=batch_size,
        thread_id=thread_id,
        stage_factory=stage_factory,
        ctx_factory=ctx_factory
    )

def _initialize_stages(stage_factory: List[Callable[[], StageInterface]],
                       ctx_factory: List[Callable[[], PipelineContext]]) -> List[
    Tuple[StageInterface, PipelineContext]]:
    if not isinstance(stage_factory, list):
        raise ValueError("stage_factory must be a list of callables producing StageInterface instances")

    if not isinstance(ctx_factory, list):
        raise ValueError("ctx_factory must be a list of callables producing PipelineContext instances")

    if len(stage_factory) != len(ctx_factory):
        raise ValueError("stage_factory and ctx_factory must have the same length")

    stages = [stage() for stage in stage_factory]
    context = [ctx() for ctx in ctx_factory]
    comb = zip(stages, context)
    s: List[Tuple[StageInterface, PipelineContext]] = list(comb)

    for i, stage, ctx in enumerate(s):
        if not isinstance(stage, StageInterface):
            raise ValueError(f"stage_factory[{i}] did not produce a StageInterface instance")
        if not isinstance(ctx, PipelineContext):
            raise ValueError(f"ctx_factory[{i}] did not produce a PipelineContext instance")
        stage.initialize(stage_id=i, ctx=ctx)
        logger.debug(f"Initialized stage {i}: {stage.__class__.__name__} with context {ctx}")

    return s


def _make_entry_stage_from_generator(data) -> StageResult[TransportRecord, str]:
    if data is None:
        raise ValueError("Data generator cannot be None")
    result = StageResult("Entry point", "Initial Stage Result with transport records")
    for record in data:
        try:
            record = process_record(record)
            result.add_ok(record)
        except Exception as e:
            result.add_err(e.__str__())
    return result
