from __future__ import annotations

from typing import Any, Iterator

from datalathe.types import ReportResultEntry, SchemaField


class DatalatheResultSet:
    """Cursor-based result set mirroring the JDBC-style interface from the JS/Java clients."""

    def __init__(self, result: ReportResultEntry):
        self._data: list[list[str | None]] = result.result or result.data or []
        self._schema: list[SchemaField] = result.schema or []
        self._current_row: int = -1
        self._was_null: bool = False

    # --- Cursor Navigation ---

    def next(self) -> bool:
        self._current_row += 1
        return self._current_row < len(self._data)

    def previous(self) -> bool:
        if self._current_row <= 0:
            return False
        self._current_row -= 1
        return True

    def first(self) -> bool:
        if not self._data:
            return False
        self._current_row = 0
        return True

    def last(self) -> bool:
        if not self._data:
            return False
        self._current_row = len(self._data) - 1
        return True

    def before_first(self) -> None:
        self._current_row = -1

    def after_last(self) -> None:
        self._current_row = len(self._data)

    def absolute(self, row: int) -> bool:
        if row < 0:
            row = len(self._data) + row + 1
        if row < 1 or row > len(self._data):
            self._current_row = len(self._data)
            return False
        self._current_row = row - 1
        return True

    def relative(self, rows: int) -> bool:
        return self.absolute(self._current_row + 1 + rows)

    # --- Position Checks ---

    def is_before_first(self) -> bool:
        return len(self._data) > 0 and self._current_row == -1

    def is_after_last(self) -> bool:
        return len(self._data) > 0 and self._current_row >= len(self._data)

    def is_first(self) -> bool:
        return len(self._data) > 0 and self._current_row == 0

    def is_last(self) -> bool:
        return len(self._data) > 0 and self._current_row == len(self._data) - 1

    def get_row(self) -> int:
        if self._current_row < 0 or self._current_row >= len(self._data):
            return 0
        return self._current_row + 1

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

    # --- Python-idiomatic extras ---

    @property
    def row_count(self) -> int:
        return len(self._data)

    def to_list(self) -> list[dict[str, Any]]:
        saved = self._current_row
        rows: list[dict[str, Any]] = []
        self.before_first()
        while self.next():
            row = {}
            for i in range(1, len(self._schema) + 1):
                row[self._schema[i - 1].name] = self.get_object(i)
            rows.append(row)
        self._current_row = saved
        return rows

    def __iter__(self) -> Iterator[dict[str, Any]]:
        self.before_first()
        while self.next():
            row = {}
            for i in range(1, len(self._schema) + 1):
                row[self._schema[i - 1].name] = self.get_object(i)
            yield row

    def __len__(self) -> int:
        return len(self._data)

    # --- Private ---

    def _resolve_column(self, column: int | str) -> int:
        return self.find_column(column) if isinstance(column, str) else column

    def _get_value(self, column_index: int) -> str | None:
        if self._current_row < 0 or self._current_row >= len(self._data):
            raise RuntimeError("No current row")
        if column_index < 1 or column_index > len(self._schema):
            raise IndexError(f"Invalid column index: {column_index}")
        return self._data[self._current_row][column_index - 1]
