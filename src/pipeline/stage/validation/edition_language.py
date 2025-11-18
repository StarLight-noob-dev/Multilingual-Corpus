from typing import Any, Dict, List, Set, override

from src.models.record.edition_record import EditionRecord
from src.models.results.stage_result import StageResult
from src.pipeline.stage.context import LanguageContext
from src.pipeline.stage.interface import StageInterface


class EditionLanguageValidationStage(StageInterface):
    @override
    def initialize(self, stage_id: str, ctx: LanguageContext, **kwargs) -> Dict[str, Any]:
        self.stage_id = stage_id
        self.stage_name = "Edition Language Validation Stage"
        #self.ctx = ctx # TODO Store context if needed later
        self.languages = ctx.flags.languages if ctx.flags.languages is not None else ctx.flags._default_languages
        return {'stage_id': self.stage_id, 'name': self.stage_name, 'languages': self.languages}

    @override
    def process_batch(self, stage_data: StageResult[EditionRecord, str],
                      ctx: LanguageContext ,**kwargs) -> StageResult[EditionRecord, str]:

        results = StageResult(self.stage_id, "Edition Language Validation Results")

        if stage_data is None:
            return results

        any_language = ctx.flags.any_language

        for record in stage_data.success_values():
            record_languages = getattr(record, 'languages', [])
            if self._is_valid_language(set(record_languages), any_language):
                results.add_ok(record)
            else:
                results.add_err(f"Invalid record: unsupported language codes: {record_languages}")

        return results

    @override
    def shutdown(self, ctx: LanguageContext) -> None:
        pass #TODO implement if needed

    def _is_valid_language(self, language_code: Set[str], any_language: bool = False) -> bool:
        """
        Check if the language code is a non-empty list, and contains at least one valid language code.

        Args:
            language_code (List[str]): The language code(s) to validate.
            any_language (bool): If True, any non-empty language code is considered valid.

        Returns:
            bool: True if valid, False otherwise.
        """
        if not language_code:
            return False
        if any_language:
            return len(language_code) >= 1
        return any(code in self.languages for code in language_code)