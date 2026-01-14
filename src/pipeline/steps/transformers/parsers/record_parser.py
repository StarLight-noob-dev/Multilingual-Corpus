from abc import abstractmethod
from typing import List, Any

from src.pipeline.steps import BaseTransformer
from src.models.record import TransportRecord


class RecordParser(BaseTransformer):
    """Extended BaseTransformer with helper methods for record parsing"""

    @abstractmethod
    def transform(self, t_record: TransportRecord) -> Any:
        pass

    def _get_str(self, data: dict, field: str, alt_fields=None) -> str:
        """
        Helper to get a string field content with optional alternative fields or fallback empty string
        """
        if alt_fields is None:
            alt_fields = []
        val = data.get(field)
        if isinstance(val, str):
            return val
        for af in alt_fields:
            v2 = data.get(af)
            if isinstance(v2, str):
                return v2
        return ""

    def _get_list(self, data: dict, field: str) -> list:
        """
        Helper to get a list field, normalizing single values into a list
        """
        val = data.get(field)
        if val is None:
            return []
        if isinstance(val, list):
            return val
        return [val]

    def _extract_key_suffix(self, data_list: List) -> List[str]:
        """
        Helper to extract the suffix from keys like '/authors/OL1A' -> 'OL1A'
        """
        result = []
        for item in data_list:
            if isinstance(item, dict) and "key" in item:
                result.append(item["key"].rsplit("/", 1)[-1])
        return result
