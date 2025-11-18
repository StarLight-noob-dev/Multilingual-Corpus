import logging
from typing import override, Dict, Any

from src.logger import get_logger
from src.models.record.edition_record import EditionRecord
from src.models.results.stage_result import StageResult
from src.models.results.types import Ok, Err
from src.pipeline.stage.context import PipelineContext
from src.pipeline.stage.interface import StageInterface


logger = get_logger(
    __name__,
    handler_level=logging.DEBUG,
)

class EditionFieldValidation(StageInterface):
    """
    Performs basic validation on incoming EditionRecords to ensure they have necessary fields populated for further
    processing.

    The validation checks include:

    - Presence of an Open Library ID (`ol_id`).
    - For editions, presence of a non-empty title.
    - If an OCAID (`ocaid`) is present, it must be a non-empty string for retrieval purposes.
    - Presence of a publishing or copyright date.

    Records failing these checks are considered invalid and are filtered out. Valid records are passed on to
    subsequent stages.
    """
    @override
    def initialize(self, stage_id: str, ctx: PipelineContext, **kwargs) -> Dict[str, Any]:
        self.stage_id: str = stage_id
        self.stage_name = "Edition Field Validation Stage"
        return {"id": stage_id, "name": self.stage_name}

    @override
    def process_batch(self, stage_data: StageResult[EditionRecord, str],
                      ctx: PipelineContext, **kwargs) -> StageResult[EditionRecord, str]:
        results = StageResult(self.stage_id, "Edition Field Validation Results")

        if stage_data is None:
            return results

        for record in stage_data.success_values():
            if self._has_necessary_attributes(record):
                results.add_ok(Ok(record))
            else:
                results.add_err(Err(f"Invalid record: missing necessary attributes: {record}"))
        logger.debug(f"{results.summary()}")
        return results

    @override
    def shutdown(self, ctx: PipelineContext) -> None:
        pass

    def _has_necessary_attributes(self, record: EditionRecord) -> bool:
        """Check if the record has necessary attributes for validation."""
        if not self._validate_string_field(record.id): return False
        if not self._validate_string_field(record.ocaid): return False
        if not self._validate_string_field(record.title): return False
        if record.publishing_date == -1 and record.copyright_date == -1 and len(record.authors) < 1:
            # If we have no date or authors, copyright validation cannot be performed
            return False
        return True

    def _validate_string_field(self, field_value: str | None) -> bool:
        """Helper method to validate that a string field is non-empty."""
        return field_value is not None and isinstance(field_value, str) and field_value.strip() != ""