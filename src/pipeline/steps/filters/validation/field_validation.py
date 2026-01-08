import logging
from typing import Any, Dict, Callable, override

from src.pipeline.steps import BaseFilter

logger = logging.getLogger(__name__)

class FieldValidationFilter(BaseFilter):

    def __init__(self, validation_map: Dict[str, Callable[[Any], bool]]) -> None:
        """
        Args:
            validation_map (Dict[str, Callable[[Any], bool]]): A dictionary where keys are field names and
                values are functions that take the field value and return a boolean.
        """
        self.validation_map = validation_map

    @override
    def filter(self, data: Any) -> bool:
        for field, check_func in self.validation_map.items():
            if isinstance(data, dict):
                if field not in data:
                    return False
                val = data[field]
            else:
                if not hasattr(data, field):
                    return False
                val = getattr(data, field)

            try:
                if not check_func(val):
                    return False
            except Exception as e:
                logger.debug(f"Validation failed for field '{field}' with error: {e}")
                return False

        return True


class EditionsNecessaryFieldsFilter(FieldValidationFilter):
    """
    A filter that validates Edition records to ensure they contain necessary fields.

    The necessary fields include:

    - `ol_id`: Must be a non-empty string.
    - `ocaid`: Must be a non-empty string. To ensure the record can be retrieved from the Internet Archive.
    """
    def __init__(self, validation_map: Dict[str, Callable[[Any], bool]] = None) -> None:
        """
        Args:
            validation_map (Dict[str, Callable[[Any], bool]]): A dictionary where keys are necessary field names
                and values are functions that validate the field values. I can be used to override or extend
                the default validation rules.
        """
        rules = {
            "ol_id": lambda x: isinstance(x, str) and len(x.strip()) > 0,
            "ocaid": lambda x: isinstance(x, str) and len(x.strip()) > 0,
            #"title": lambda x: isinstance(x, str) and len(x.strip()) > 0,
            #"publishing_date": lambda x: isinstance(x, int) and x != -1,
        }
        # TODO consider adding title and publishing_date

        # Override or extend the default rules with any provided validation_map
        if validation_map:
            rules.update(validation_map)
        super().__init__(validation_map=rules)