from typing import override, Dict, Any

from src.models.record.edition_record import EditionRecord
from src.models.results.stage_result import StageResult
from src.models.results.types import Ok, Err
from src.stage.context import PipelineContext
from src.stage.interface import StageInterface


class EditionFieldValidation(StageInterface):
    """
    Performs basic validation on incoming records to ensure they have necessary fields populated for further
    processing.

    The validation checks include:

    - Presence of an Open Library ID (`ol_id`).
    - For editions, presence of a non-empty title.
    - If an OCAID (`ocaid`) is present, it must be a non-empty string for retrieval purposes.
    - Presence of a publishing date.

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

        # TODO Add context write file before dispatching to next stage
        return results

    @override
    def shutdown(self, ctx: PipelineContext) -> None:
        pass

    def _has_necessary_attributes(self, record: EditionRecord) -> bool:
        """Check if the record has necessary attributes for validation."""
        ol_id = getattr(record, 'ol_id', None)
        ocaid = getattr(record, 'ocaid', None)
        title = getattr(record, 'title', None)
        publishing_date = getattr(record, 'publishing_date', None)
        return (ol_id is not None and
                (title is None or (isinstance(title, str) and title.strip() != "")) and
                (ocaid is None or (isinstance(ocaid, str) and ocaid.strip() != "")) and
                publishing_date is not None)