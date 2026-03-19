from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class SourceType(str, Enum):
    MYSQL = "MYSQL"
    FILE = "FILE"
    S3 = "S3"
    LOCAL = "LOCAL"
    CACHE = "CACHE"


class ReportType(str, Enum):
    GENERIC = "Generic"
    TABLE = "Table"


@dataclass
class SchemaField:
    name: str
    data_type: str


@dataclass
class Partition:
    partition_by: str
    partition_values: list[str] | None = None
    partition_query: str | None = None
    combine_partitions: bool | None = None


@dataclass
class SourceRequest:
    database_name: str = ""
    query: str = ""
    table_name: str | None = None
    file_path: str | None = None
    s3_path: str | None = None
    source_chip_ids: list[str] | None = None
    partition: Partition | None = None
    column_replace: dict[str, str] | None = None


@dataclass
class S3StorageConfig:
    bucket: str | None = None
    key_prefix: str | None = None
    ttl_days: int | None = None


@dataclass
class StageDataRequest:
    source_type: SourceType
    source_request: SourceRequest
    chip_id: str | None = None
    chip_name: str | None = None
    storage_config: S3StorageConfig | None = None
    tags: dict[str, str] | None = None


@dataclass
class StageDataResponse:
    chip_id: str
    error: str | None = None


@dataclass
class QueryRequest:
    query: list[str]
    file_path: str | None = None


@dataclass
class ReportRequest:
    chip_id: list[str]
    source_type: SourceType
    type: ReportType
    query_request: QueryRequest
    transform_query: bool | None = None
    return_transformed_query: bool | None = None


@dataclass
class ReportResultEntry:
    idx: str
    result: list[list[str | None]] | None = None
    data: list[list[str | None]] | None = None
    error: str | None = None
    schema: list[SchemaField] | None = None
    transformed_query: str | None = None


@dataclass
class ReportTiming:
    total_ms: float
    chip_attach_ms: float
    query_execution_ms: float


@dataclass
class ReportResponse:
    result: dict[str, ReportResultEntry] | None = None
    error: str | None = None
    timing: ReportTiming | None = None


@dataclass
class DuckDBDatabase:
    database_name: str
    database_oid: int
    internal: bool
    type: str
    readonly: bool
    path: str | None = None
    comment: str | None = None
    tags: str | None = None


@dataclass
class DatabaseTable:
    table_name: str
    schema_name: str
    column_name: str
    data_type: str
    is_nullable: str
    ordinal_position: int
    column_default: str | None = None


@dataclass
class Chip:
    chip_id: str
    sub_chip_id: str
    table_name: str
    partition_value: str
    created_at: int | None = None


@dataclass
class ChipMetadata:
    chip_id: str
    created_at: int
    description: str
    name: str
    query: str | None = None
    tables: str | None = None
    storage_bucket: str | None = None
    storage_key_prefix: str | None = None
    ttl_days: int | None = None


@dataclass
class ChipTag:
    chip_id: str
    key: str
    value: str


@dataclass
class ChipsResponse:
    chips: list[Chip] = field(default_factory=list)
    metadata: list[ChipMetadata] = field(default_factory=list)
    tags: list[ChipTag] | None = None


def _to_dict(obj: Any) -> Any:
    """Recursively convert dataclasses/enums to JSON-serializable dicts."""
    if isinstance(obj, Enum):
        return obj.value
    if hasattr(obj, "__dataclass_fields__"):
        result = {}
        for k in obj.__dataclass_fields__:
            v = getattr(obj, k)
            if v is not None:
                result[k] = _to_dict(v)
        return result
    if isinstance(obj, list):
        return [_to_dict(i) for i in obj]
    if isinstance(obj, dict):
        return {k: _to_dict(v) for k, v in obj.items()}
    return obj


def _from_dict(cls: type, data: dict) -> Any:
    """Create a dataclass instance from a dict, handling nested types."""
    if not isinstance(data, dict):
        return data

    hints = getattr(cls, "__dataclass_fields__", {})
    kwargs = {}
    for name, f in hints.items():
        if name not in data:
            continue
        value = data[name]
        annotation = f.type

        # Resolve the actual type for nested dataclasses
        if isinstance(annotation, str):
            # Evaluate forward references
            import datalathe.types as _mod
            annotation = eval(annotation, vars(_mod))

        origin = getattr(annotation, "__origin__", None)

        if isinstance(value, dict) and hasattr(annotation, "__dataclass_fields__"):
            kwargs[name] = _from_dict(annotation, value)
        elif origin is list and isinstance(value, list):
            args = getattr(annotation, "__args__", ())
            if args and hasattr(args[0], "__dataclass_fields__"):
                kwargs[name] = [_from_dict(args[0], item) for item in value]
            else:
                kwargs[name] = value
        elif isinstance(annotation, type) and issubclass(annotation, Enum) and value is not None:
            kwargs[name] = annotation(value)
        else:
            kwargs[name] = value

    return cls(**kwargs)
