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


class DatalatheQueryError(DatalatheError):
    """Raised when one or more queries in a generate_report call fail at
    execution time. The engine returns HTTP 200 with these errors in the
    per-query `error` field; without this exception they would be silently
    swallowed as empty results.

    Pass ``raise_on_query_error=False`` to generate_report to suppress this
    and inspect ``ReportResultEntry.error`` on the returned result instead.
    """

    def __init__(self, errors: dict[int, str]):
        self.errors = errors
        detail = "; ".join(f"query {idx}: {msg}" for idx, msg in sorted(errors.items()))
        super().__init__(f"Query execution failed ({detail})")


class DatalatheIngestError(DatalatheError):
    """Raised by wait_for_ingest when an async ingest job ends failed or
    cancelled. ``job`` is the final job record; ``job.error`` carries the
    engine's failure detail when available.
    """

    def __init__(self, message: str, job=None):
        super().__init__(message)
        self.job = job


class DatalatheIngestTimeoutError(DatalatheError):
    """Raised by wait_for_ingest when the job has not reached a terminal
    state within the timeout. ``job`` is the last-observed job record.
    """

    def __init__(self, message: str, job=None):
        super().__init__(message)
        self.job = job


class ChipNotFoundError(DatalatheApiError):
    """Raised when a request references a chip whose data is no longer available
    (typically because the underlying S3 object has expired via lifecycle policy).

    Recovery pattern: catch this exception, re-stage the chip from your own
    source-of-truth using the same chip_id, then retry the original call.
    """

    def __init__(self, message: str, chip_id: str | None, response_body: str | None = None):
        super().__init__(message, 404, response_body)
        self.chip_id = chip_id
