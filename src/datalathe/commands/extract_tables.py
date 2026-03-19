from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class ExtractTablesResponse:
    tables: list[str]
    transformed_query: str | None = None
    error: str | None = None


class ExtractTablesCommand:
    endpoint = "/lathe/query/tables"

    def __init__(self, query: str, transform: bool | None = None):
        req: dict[str, Any] = {"query": query}
        if transform is not None:
            req["transform"] = transform
        self._request = req

    @property
    def request(self) -> dict[str, Any]:
        return self._request

    def parse_response(self, json_data: Any) -> ExtractTablesResponse:
        return ExtractTablesResponse(
            tables=json_data.get("tables", []),
            transformed_query=json_data.get("transformed_query"),
            error=json_data.get("error"),
        )
