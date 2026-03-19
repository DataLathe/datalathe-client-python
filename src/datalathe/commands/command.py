from __future__ import annotations

from typing import Any, Protocol


class DatalatheCommand(Protocol):
    @property
    def endpoint(self) -> str: ...

    @property
    def request(self) -> dict[str, Any]: ...

    def parse_response(self, json_data: Any) -> Any: ...
