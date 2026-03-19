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
