from __future__ import annotations

from typing import Any

from datalathe.types import (
    SourceType,
    ReportType,
    ReportResponse,
    ReportResultEntry,
    ReportTiming,
    SchemaField,
)


class GenerateReportCommand:
    endpoint = "/lathe/report"

    def __init__(
        self,
        chip_ids: list[str],
        source_type: SourceType,
        queries: list[str],
        report_type: ReportType = ReportType.GENERIC,
        transform_query: bool | None = None,
        return_transformed_query: bool | None = None,
    ):
        req: dict[str, Any] = {
            "chip_id": chip_ids,
            "source_type": source_type.value,
            "type": report_type.value,
            "query_request": {"query": queries},
        }
        if transform_query is not None:
            req["transform_query"] = transform_query
        if return_transformed_query is not None:
            req["return_transformed_query"] = return_transformed_query
        self._request = req

    @property
    def request(self) -> dict[str, Any]:
        return self._request

    def parse_response(self, json_data: Any) -> ReportResponse:
        result_map = None
        if json_data.get("result"):
            result_map = {}
            for key, entry in json_data["result"].items():
                schema = None
                if entry.get("schema"):
                    schema = [SchemaField(name=s["name"], data_type=s["data_type"]) for s in entry["schema"]]
                result_map[key] = ReportResultEntry(
                    idx=entry.get("idx", key),
                    result=entry.get("result"),
                    data=entry.get("data"),
                    error=entry.get("error"),
                    schema=schema,
                    transformed_query=entry.get("transformed_query"),
                )

        timing = None
        if json_data.get("timing"):
            t = json_data["timing"]
            timing = ReportTiming(
                total_ms=t["total_ms"],
                chip_attach_ms=t["chip_attach_ms"],
                query_execution_ms=t["query_execution_ms"],
            )

        return ReportResponse(
            result=result_map,
            error=json_data.get("error"),
            timing=timing,
        )
