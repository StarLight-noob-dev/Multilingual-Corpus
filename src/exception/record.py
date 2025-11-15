class UnknownRecordTypeError(Exception):
    """Exception raised for unknown record types."""

    def __init__(self, record_type: str):
        self.record_type = record_type
        self.message = f"Unknown record type: {self.record_type}"
        super().__init__(self.message)

class RecordConversionError(Exception):
    """Exception raised for errors during record conversion."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)