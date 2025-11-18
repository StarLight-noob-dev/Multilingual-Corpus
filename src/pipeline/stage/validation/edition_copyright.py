from datetime import date
from typing import Any, Dict

from src.models.record.edition_record import EditionRecord
from src.models.results.stage_result import StageResult
from src.pipeline.stage.context import PipelineContext
from src.pipeline.stage.interface import StageInterface


class CopyrightValidator(StageInterface):

    def initialize(self, stage_id: str, ctx: PipelineContext, **kwargs) -> Dict[str, Any]:
        pass

    def process_batch(self, stage_data: StageResult, ctx: PipelineContext, **kwargs) -> StageResult | Any | None:
        pass

    def shutdown(self, ctx: PipelineContext) -> None:
        pass

    def _validate(self, edition: EditionRecord) -> bool:

        publishing_date = getattr(edition, "publishing_date", None)
        copyright_date = getattr(edition, "copyright_date", None)

        if copyright_date:
            try:
                edition.copyright_year = int(copyright_date)
            except ValueError:
                edition.copyright_year = -1

        return True

    

    def is_copyrighted_in_germany(self,
            author_death_year: int | None = None,
            publication_year: int | None = None,
            copyright_year: int | None = None,
            authors: List[str] | None = None,
    ) -> tuple[bool, str]:

        current_year = date.today().year

        # Case 1: Author is known — apply 70 years after death
        if author_death_year:
            expiry_year = author_death_year + 70
            if current_year > expiry_year:
                return (False, f"Copyright expired on Dec 31, {expiry_year}. Now public domain in Germany.")
            else:
                return (True, f"Protected until Dec 31, {expiry_year} (70 years after author's death).")

        # Case 3: Anonymous/pseudonymous works — 70 years from publication
        if authors and publication_year:
            expiry_year = publication_year + 70
            if current_year > expiry_year:
                return (False, f"Anonymous work: copyright expired in {expiry_year}.")
            else:
                return (True, f"Anonymous work: protected until Dec 31, {expiry_year}.")

        # Case 4: Fallback: old publication year
        if publication_year and publication_year < 1925:
            return (False, f"Published before 1925 — almost certainly public domain in Germany.")

        return (True, "Insufficient data — likely still protected unless proven otherwise.")