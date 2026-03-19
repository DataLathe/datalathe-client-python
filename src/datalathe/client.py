from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlencode

import requests

from datalathe.commands.command import DatalatheCommand
from datalathe.commands.create_chip import CreateChipCommand
from datalathe.commands.extract_tables import ExtractTablesCommand
from datalathe.commands.generate_report import GenerateReportCommand
from datalathe.errors import DatalatheApiError, DatalatheStageError
from datalathe.types import (
    Chip,
    ChipMetadata,
    ChipTag,
    ChipsResponse,
    DatabaseTable,
    DuckDBDatabase,
    Partition,
    ReportResultEntry,
    ReportTiming,
    S3StorageConfig,
    SchemaField,
    SourceRequest,
    SourceType,
    _from_dict,
)


@dataclass
class GenerateReportResult:
    results: dict[int, ReportResultEntry]
    timing: ReportTiming | None


class DatalatheClient:
    def __init__(
        self,
        base_url: str,
        headers: dict[str, str] | None = None,
        timeout: float = 30.0,
    ):
        self._base_url = base_url.rstrip("/")
        self._headers = headers or {}
        self._timeout = timeout
        self._session = requests.Session()
        self._session.headers.update(self._headers)

    # --- Chip creation ---

    def create_chip(
        self,
        source_name: str,
        query: str,
        table_name: str,
        partition: Partition | None = None,
        chip_name: str | None = None,
        column_replace: dict[str, str] | None = None,
        storage_config: S3StorageConfig | None = None,
    ) -> str:
        chips = self.create_chips(
            sources=[SourceRequest(
                database_name=source_name,
                table_name=table_name,
                query=query,
                partition=partition,
                column_replace=column_replace,
            )],
            source_type=SourceType.MYSQL,
            chip_name=chip_name,
            storage_config=storage_config,
        )
        return chips[0]

    def create_chip_from_file(
        self,
        file_path: str,
        table_name: str | None = None,
        partition: Partition | None = None,
        chip_name: str | None = None,
        column_replace: dict[str, str] | None = None,
        storage_config: S3StorageConfig | None = None,
    ) -> str:
        chips = self.create_chips(
            sources=[SourceRequest(
                database_name="",
                query="",
                file_path=file_path,
                table_name=table_name,
                partition=partition,
                column_replace=column_replace,
            )],
            source_type=SourceType.FILE,
            chip_name=chip_name,
            storage_config=storage_config,
        )
        return chips[0]

    def create_chip_from_chip(
        self,
        source_chip_ids: list[str],
        query: str | None = None,
        table_name: str | None = None,
        chip_name: str | None = None,
        storage_config: S3StorageConfig | None = None,
    ) -> str:
        chips = self.create_chips(
            sources=[SourceRequest(
                database_name="",
                query=query or "",
                source_chip_ids=source_chip_ids,
                table_name=table_name,
            )],
            source_type=SourceType.CACHE,
            chip_name=chip_name,
            storage_config=storage_config,
        )
        return chips[0]

    def create_chips(
        self,
        sources: list[SourceRequest],
        chip_id: str | None = None,
        source_type: SourceType = SourceType.MYSQL,
        chip_name: str | None = None,
        storage_config: S3StorageConfig | None = None,
        tags: dict[str, str] | None = None,
    ) -> list[str]:
        chip_ids: list[str] = []
        for source in sources:
            command = CreateChipCommand(
                source_type=source_type,
                source=source,
                chip_id=chip_id,
                chip_name=chip_name,
                storage_config=storage_config,
                tags=tags,
            )
            response = self.send_command(command)
            if response.error:
                raise DatalatheStageError(f"Failed to stage data: {response.error}")
            chip_ids.append(response.chip_id)
        return chip_ids

    # --- Query / Report ---

    def generate_report(
        self,
        chip_ids: list[str],
        queries: list[str],
        source_type: SourceType = SourceType.LOCAL,
        transform_query: bool | None = None,
        return_transformed_query: bool | None = None,
    ) -> GenerateReportResult:
        command = GenerateReportCommand(
            chip_ids=chip_ids,
            source_type=source_type,
            queries=queries,
            transform_query=transform_query,
            return_transformed_query=return_transformed_query,
        )
        response = self.send_command(command)
        results: dict[int, ReportResultEntry] = {}
        if response.result:
            for key, entry in response.result.items():
                results[int(key)] = entry
        return GenerateReportResult(results=results, timing=response.timing)

    # --- Database inspection ---

    def get_databases(self) -> list[DuckDBDatabase]:
        data = self._get("/lathe/stage/databases")
        return [_from_dict(DuckDBDatabase, d) for d in data]

    def get_database_schema(self, database_name: str) -> list[DatabaseTable]:
        data = self._get(f"/lathe/stage/schema/{quote(database_name, safe='')}")
        return [_from_dict(DatabaseTable, d) for d in data]

    # --- Chip metadata & tagging ---

    def list_chips(self) -> ChipsResponse:
        return self._parse_chips_response(self._get("/lathe/chips"))

    def search_chips(
        self,
        table_name: str | None = None,
        partition_value: str | None = None,
        tag: str | None = None,
    ) -> ChipsResponse:
        params: dict[str, str] = {}
        if table_name is not None:
            params["table_name"] = table_name
        if partition_value is not None:
            params["partition_value"] = partition_value
        if tag is not None:
            params["tag"] = tag
        qs = urlencode(params)
        path = f"/lathe/chips/search?{qs}" if qs else "/lathe/chips/search"
        return self._parse_chips_response(self._get(path))

    def add_chip_tags(self, chip_id: str, tags: dict[str, str]) -> None:
        self._post(f"/lathe/chips/{quote(chip_id, safe='')}/tags", {"tags": tags})

    def delete_chip_tag(self, chip_id: str, key: str) -> None:
        self._delete(f"/lathe/chips/{quote(chip_id, safe='')}/tags/{quote(key, safe='')}")

    def delete_chip(self, chip_id: str) -> None:
        self._delete(f"/lathe/chips/{quote(chip_id, safe='')}")

    # --- Query analysis ---

    def extract_tables(self, query: str) -> list[str]:
        resp = self.extract_tables_with_transform(query)
        return resp["tables"]

    def extract_tables_with_transform(
        self,
        query: str,
        transform: bool | None = None,
    ) -> dict[str, Any]:
        command = ExtractTablesCommand(query, transform)
        response = self.send_command(command)
        if response.error:
            raise DatalatheApiError(
                f"Failed to extract tables: {response.error}",
                400,
                response.error,
            )
        return {"tables": response.tables, "transformed_query": response.transformed_query}

    # --- Raw / generic ---

    def stage_data(self, request: dict[str, Any]) -> Any:
        return self._post("/lathe/stage/data", request)

    def post_report(self, request: dict[str, Any]) -> Any:
        return self._post("/lathe/report", request)

    def send_command(self, command: DatalatheCommand) -> Any:
        url = self._base_url + command.endpoint
        resp = self._session.post(
            url,
            json=command.request,
            headers={"Content-Type": "application/json"},
            timeout=self._timeout,
        )
        if not resp.ok:
            raise DatalatheApiError(
                f"Failed to execute command: {resp.status_code} {resp.text}",
                resp.status_code,
                resp.text,
            )
        return command.parse_response(resp.json())

    # --- Profiler methods ---

    def get_profiler_tables(self) -> list[dict[str, Any]]:
        return self._get("/lathe/profiler/tables")

    def start_profiler(self, skip_files: bool) -> Any:
        return self._get(f"/lathe/profiler/start/{str(skip_files).lower()}")

    def get_table_description(self, table_id: str) -> list[Any]:
        return self._get(f"/lathe/profiler/table/{quote(table_id, safe='')}/describe")

    def get_table_data(self, table_id: str) -> list[Any]:
        return self._get(f"/lathe/profiler/table/{quote(table_id, safe='')}")

    def get_table_source_files(self, table_id: str) -> list[Any]:
        return self._get(f"/lathe/profiler/table/{quote(table_id, safe='')}/source_file")

    def get_table_summary(self, table_id: str) -> Any:
        return self._get(f"/lathe/profiler/table/{quote(table_id, safe='')}/summary")

    def get_profiler_config(self) -> dict[str, Any]:
        return self._get("/lathe/profiler/config")

    def update_profiler_config(self, config: dict[str, Any]) -> Any:
        return self._post("/lathe/profiler/config/update", config)

    def get_schema_mappings(self) -> list[dict[str, Any]]:
        return self._get("/lathe/profiler/schema/mappings")

    def get_profiler_schema(self, request: dict[str, Any]) -> Any:
        return self._post("/lathe/profiler/schema", request)

    # --- Source / Job methods ---

    def get_source_file(self, file_id: str) -> dict[str, Any]:
        return self._get(f"/lathe/source/file/{quote(file_id, safe='')}")

    def get_all_jobs(self) -> dict[str, Any]:
        return self._get("/lathe/jobs/all")

    # --- Private HTTP methods ---

    def _get(self, path: str) -> Any:
        url = self._base_url + path
        resp = self._session.get(url, timeout=self._timeout)
        if not resp.ok:
            raise DatalatheApiError(
                f"GET {path} failed: {resp.status_code} {resp.text}",
                resp.status_code,
                resp.text,
            )
        return resp.json()

    def _post(self, path: str, body: Any) -> Any:
        url = self._base_url + path
        resp = self._session.post(
            url,
            json=body,
            headers={"Content-Type": "application/json"},
            timeout=self._timeout,
        )
        if not resp.ok:
            raise DatalatheApiError(
                f"POST {path} failed: {resp.status_code} {resp.text}",
                resp.status_code,
                resp.text,
            )
        return resp.json()

    def _delete(self, path: str) -> None:
        url = self._base_url + path
        resp = self._session.delete(url, timeout=self._timeout)
        if not resp.ok:
            raise DatalatheApiError(
                f"DELETE {path} failed: {resp.status_code} {resp.text}",
                resp.status_code,
                resp.text,
            )

    @staticmethod
    def _parse_chips_response(data: dict[str, Any]) -> ChipsResponse:
        chips = [_from_dict(Chip, c) for c in data.get("chips", [])]
        metadata = [_from_dict(ChipMetadata, m) for m in data.get("metadata", [])]
        tags = None
        if data.get("tags") is not None:
            tags = [_from_dict(ChipTag, t) for t in data["tags"]]
        return ChipsResponse(chips=chips, metadata=metadata, tags=tags)
