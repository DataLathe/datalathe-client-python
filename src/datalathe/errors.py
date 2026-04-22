class DatalatheError(Exception):
    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class DatalatheApiError(DatalatheError):
    def __init__(self, message: str, status_code: int, response_body: str | None = None):
        super().__init__(message, status_code)
        self.response_body = response_body


class DatalatheStageError(DatalatheError):
    def __init__(self, message: str):
        super().__init__(message)


class ChipNotFoundError(DatalatheApiError):
    """Raised when a request references a chip whose data is no longer available
    (typically because the underlying S3 object has expired via lifecycle policy).

    Recovery pattern: catch this exception, re-stage the chip from your own
    source-of-truth using the same chip_id, then retry the original call.
    """

    def __init__(self, message: str, chip_id: str | None, response_body: str | None = None):
        super().__init__(message, 404, response_body)
        self.chip_id = chip_id
