from __future__ import annotations

from typing import Any

from datalathe.types import (
    SourceType,
    SourceRequest,
    S3StorageConfig,
    StageDataResponse,
    _to_dict,
)


class CreateChipCommand:
    endpoint = "/lathe/stage/data"

    def __init__(
        self,
        source_type: SourceType,
        source: SourceRequest,
        chip_id: str | None = None,
        chip_name: str | None = None,
        storage_config: S3StorageConfig | None = None,
        tags: dict[str, str] | None = None,
    ):
        req: dict[str, Any] = {
            "source_type": source_type.value,
            "source_request": _to_dict(source),
        }
        if chip_id is not None:
            req["chip_id"] = chip_id
        if chip_name is not None:
            req["chip_name"] = chip_name
        if storage_config is not None:
            req["storage_config"] = _to_dict(storage_config)
        if tags is not None:
            req["tags"] = tags
        self._request = req

    @property
    def request(self) -> dict[str, Any]:
        return self._request

    def parse_response(self, json_data: Any) -> StageDataResponse:
        return StageDataResponse(
            chip_id=json_data["chip_id"],
            error=json_data.get("error"),
            total_rows=json_data.get("total_rows"),
            elapsed_ms=json_data.get("elapsed_ms"),
        )
