from __future__ import annotations

import json
from typing import Any, Iterator

from datalathe.errors import DatalatheApiError, DatalatheQueryError
from datalathe.types import SchemaField


class DatalatheStreamingResultSet:
    """Forward-only cursor over a streamed report result (NDJSON over POST
    /lathe/report with ``stream: true``).

    Mirrors the accessor surface of DatalatheResultSet — ``next()``, the
    ``get_*`` accessors, ``__iter__``, and schema access — but never buffers
    the whole result: frames are pulled and parsed lazily as the cursor
    advances. Backward navigation (``previous``/``first``/``last``/
    ``absolute``) is unsupported. ``row_count`` is unavailable until the
    terminal ``end`` frame has been consumed.
    """

    def __init__(self, lines: Iterator[bytes | str]):
        self._lines = lines
        self._schema: list[SchemaField] = []
        self._transformed_query: str | None = None
        self._row_buffer: list[list[str | None]] = []
        self._buffer_pos: int = 0
        self._current_row: list[str | None] | None = None
        self._rows_seen: int = 0
        self._row_count: int | None = None
        self._timing: dict[str, Any] | None = None
        self._terminated: bool = False
        self._exhausted: bool = False
        self._was_null: bool = False
        self._read_schema()

    # --- Cursor Navigation ---

    def next(self) -> bool:
        if self._buffer_pos < len(self._row_buffer):
            self._current_row = self._row_buffer[self._buffer_pos]
            self._buffer_pos += 1
            return True
        if self._exhausted:
            self._current_row = None
            return False
        if not self._pull_rows():
            self._current_row = None
            return False
        self._current_row = self._row_buffer[self._buffer_pos]
        self._buffer_pos += 1
        return True

    def previous(self) -> bool:
        raise self._forward_only("previous")

    def first(self) -> bool:
        raise self._forward_only("first")

    def last(self) -> bool:
        raise self._forward_only("last")

    def absolute(self, row: int) -> bool:
        raise self._forward_only("absolute")

    def relative(self, rows: int) -> bool:
        raise self._forward_only("relative")

    # --- Value Accessors (1-based column index) ---

    def get_string(self, column: int | str) -> str | None:
        value = self._get_value(self._resolve_column(column))
        self._was_null = value is None
        return value

    def get_int(self, column: int | str) -> int:
        value = self._get_value(self._resolve_column(column))
        self._was_null = value is None
        return 0 if value is None else int(float(value))

    def get_float(self, column: int | str) -> float:
        value = self._get_value(self._resolve_column(column))
        self._was_null = value is None
        return 0.0 if value is None else float(value)

    def get_double(self, column: int | str) -> float:
        return self.get_float(column)

    def get_boolean(self, column: int | str) -> bool:
        value = self._get_value(self._resolve_column(column))
        self._was_null = value is None
        return value is not None and value.lower() == "true"

    def get_object(self, column: int | str) -> str | int | float | bool | None:
        col_idx = self._resolve_column(column)
        value = self._get_value(col_idx)
        self._was_null = value is None
        if value is None:
            return None

        data_type = self._schema[col_idx - 1].data_type
        if data_type in ("Int32", "Int64"):
            return int(float(value))
        if data_type in ("Float32", "Float64"):
            return float(value)
        if data_type == "Boolean":
            return value.lower() == "true"
        return value

    def was_null(self) -> bool:
        return self._was_null

    # --- Column Lookup ---

    def find_column(self, column_label: str) -> int:
        lower = column_label.lower()
        for i, s in enumerate(self._schema):
            if s.name.lower() == lower:
                return i + 1
        raise ValueError(f"Column not found: {column_label}")

    # --- Metadata ---

    def get_column_count(self) -> int:
        return len(self._schema)

    def get_column_name(self, column_index: int) -> str:
        return self._schema[column_index - 1].name

    def get_column_type(self, column_index: int) -> str:
        return self._schema[column_index - 1].data_type

    def get_schema(self) -> list[SchemaField]:
        return list(self._schema)

    @property
    def transformed_query(self) -> str | None:
        return self._transformed_query

    @property
    def timing(self) -> dict[str, Any] | None:
        return self._timing

    # --- Python-idiomatic extras ---

    @property
    def row_count(self) -> int | None:
        """The total row count, available only after the stream is fully
        consumed (the terminal ``end`` frame carries it). Returns None while
        rows are still pending."""
        return self._row_count

    def __iter__(self) -> Iterator[dict[str, Any]]:
        while self.next():
            row = {}
            for i in range(1, len(self._schema) + 1):
                row[self._schema[i - 1].name] = self.get_object(i)
            yield row

    # --- Private ---

    @staticmethod
    def _forward_only(method: str) -> Exception:
        return NotImplementedError(
            f"{method}() is not supported on streaming results; "
            "DatalatheStreamingResultSet is forward-only"
        )

    def _resolve_column(self, column: int | str) -> int:
        return self.find_column(column) if isinstance(column, str) else column

    def _get_value(self, column_index: int) -> str | None:
        if self._current_row is None:
            raise RuntimeError("No current row")
        if column_index < 1 or column_index > len(self._schema):
            raise IndexError(f"Invalid column index: {column_index}")
        return self._current_row[column_index - 1]

    def _next_frame(self) -> dict[str, Any] | None:
        for line in self._lines:
            if isinstance(line, bytes):
                line = line.decode("utf-8")
            if not line:
                continue
            try:
                return json.loads(line)
            except json.JSONDecodeError as exc:
                raise DatalatheApiError(
                    f"Malformed frame in streamed report response: {line!r} ({exc})",
                    200,
                    line,
                )
        return None

    def _read_schema(self) -> None:
        frame = self._next_frame()
        if frame is None:
            raise DatalatheApiError(
                "Streamed report response ended before the schema frame",
                200,
                None,
            )
        ftype = frame.get("type")
        if ftype == "error":
            self._raise_error_frame(frame)
        if ftype != "schema":
            raise DatalatheApiError(
                f"Expected a schema frame first, got {ftype!r}",
                200,
                json.dumps(frame),
            )
        self._schema = [
            SchemaField(name=s["name"], data_type=s["data_type"])
            for s in frame.get("schema", [])
        ]
        self._transformed_query = frame.get("transformed_query")

    def _pull_rows(self) -> bool:
        """Pulls frames until a rows frame yields data or a terminal frame is
        seen. Returns True if the row buffer was refilled, False at clean end."""
        while True:
            frame = self._next_frame()
            if frame is None:
                self._exhausted = True
                if not self._terminated:
                    raise DatalatheApiError(
                        "Streamed report response ended without a terminal frame "
                        "(transport error); the result is incomplete",
                        200,
                        None,
                    )
                return False
            ftype = frame.get("type")
            if ftype == "rows":
                rows = frame.get("rows") or []
                if not rows:
                    continue
                self._row_buffer = rows
                self._buffer_pos = 0
                self._rows_seen += len(rows)
                return True
            if ftype == "end":
                self._terminated = True
                self._exhausted = True
                self._row_count = frame.get("row_count", self._rows_seen)
                self._timing = frame.get("timing")
                return False
            if ftype == "error":
                self._terminated = True
                self._exhausted = True
                self._raise_error_frame(frame)
            raise DatalatheApiError(
                f"Unexpected frame type in streamed report response: {ftype!r}",
                200,
                json.dumps(frame),
            )

    @staticmethod
    def _raise_error_frame(frame: dict[str, Any]) -> None:
        message = frame.get("error") or "Streamed query failed"
        raise DatalatheQueryError({0: message})
