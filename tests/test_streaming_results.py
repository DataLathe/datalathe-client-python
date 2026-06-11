"""Tests for streaming report-result consumption (generate_report_stream).

The engine streams a single query's result as NDJSON over POST /lathe/report
with ``stream: true``: a schema frame, zero-or-more rows frames (one per record
batch), then a terminal end or error frame. These tests lock the client's
forward-only cursor contract and its handling of every terminal shape.
"""
from __future__ import annotations

import json

import pytest
import responses

from datalathe import (
    DatalatheApiError,
    DatalatheClient,
    DatalatheQueryError,
    DatalatheStreamingResultSet,
)


BASE = "http://localhost:8080"


def _ndjson(*frames: dict) -> str:
    return "\n".join(json.dumps(f) for f in frames)


_SCHEMA = {
    "type": "schema",
    "schema": [
        {"name": "name", "data_type": "Utf8"},
        {"name": "age", "data_type": "Int32"},
    ],
}


def _register(body: str) -> None:
    responses.add(
        responses.POST,
        f"{BASE}/lathe/report",
        body=body,
        status=200,
        content_type="application/x-ndjson",
    )


@responses.activate
def test_stream_happy_path() -> None:
    _register(
        _ndjson(
            _SCHEMA,
            {"type": "rows", "rows": [["Alice", "30"], ["Bob", "25"]]},
            {"type": "rows", "rows": [["Charlie", "40"]]},
            {"type": "end", "row_count": 3, "timing": {"total_ms": 12}},
        )
    )

    client = DatalatheClient(BASE)
    rs = client.generate_report_stream(["chip1"], ["SELECT name, age FROM t"])

    assert isinstance(rs, DatalatheStreamingResultSet)
    assert rs.row_count is None

    assert rs.next()
    assert rs.get_string("name") == "Alice"
    assert rs.get_int("age") == 30
    assert rs.next()
    assert rs.get_string(1) == "Bob"
    assert rs.next()
    assert rs.get_string("name") == "Charlie"
    assert rs.get_int("age") == 40
    assert not rs.next()

    assert rs.row_count == 3
    schema = rs.get_schema()
    assert [s.name for s in schema] == ["name", "age"]
    assert rs.get_column_count() == 2
    assert rs.timing == {"total_ms": 12}


@responses.activate
def test_stream_iterates_as_dicts() -> None:
    _register(
        _ndjson(
            _SCHEMA,
            {"type": "rows", "rows": [["Alice", "30"], ["Bob", "25"]]},
            {"type": "end", "row_count": 2, "timing": {}},
        )
    )

    client = DatalatheClient(BASE)
    rs = client.generate_report_stream(["chip1"], ["SELECT * FROM t"])

    rows = list(rs)
    assert rows == [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
    assert rs.row_count == 2


@responses.activate
def test_stream_schema_available_before_rows() -> None:
    _register(
        _ndjson(
            {**_SCHEMA, "transformed_query": "SELECT name, age FROM lathe_t"},
            {"type": "rows", "rows": [["Alice", "30"]]},
            {"type": "end", "row_count": 1, "timing": {}},
        )
    )

    client = DatalatheClient(BASE)
    rs = client.generate_report_stream(["chip1"], ["SELECT * FROM t"])

    assert rs.get_column_name(1) == "name"
    assert rs.get_column_type(2) == "Int32"
    assert rs.transformed_query == "SELECT name, age FROM lathe_t"


@responses.activate
def test_stream_empty_result() -> None:
    _register(_ndjson(_SCHEMA, {"type": "end", "row_count": 0, "timing": {}}))

    client = DatalatheClient(BASE)
    rs = client.generate_report_stream(["chip1"], ["SELECT * FROM t WHERE 1=0"])

    assert not rs.next()
    assert rs.row_count == 0
    assert rs.get_column_count() == 2


@responses.activate
def test_stream_forward_only_methods_raise() -> None:
    _register(
        _ndjson(
            _SCHEMA,
            {"type": "rows", "rows": [["Alice", "30"]]},
            {"type": "end", "row_count": 1, "timing": {}},
        )
    )

    client = DatalatheClient(BASE)
    rs = client.generate_report_stream(["chip1"], ["SELECT * FROM t"])

    for method in (rs.previous, rs.first, rs.last):
        with pytest.raises(NotImplementedError, match="forward-only"):
            method()
    with pytest.raises(NotImplementedError, match="forward-only"):
        rs.absolute(1)
    with pytest.raises(NotImplementedError, match="forward-only"):
        rs.relative(1)


def test_stream_multi_query_rejected_client_side() -> None:
    client = DatalatheClient(BASE)
    with pytest.raises(DatalatheApiError) as excinfo:
        client.generate_report_stream(["chip1"], ["SELECT 1", "SELECT 2"])
    assert excinfo.value.status_code == 400


@responses.activate
def test_stream_error_frame_raises_query_error() -> None:
    _register(
        _ndjson(
            _SCHEMA,
            {"type": "rows", "rows": [["Alice", "30"]]},
            {
                "type": "error",
                "error": "Conversion Error: cast failed mid-scan",
                "error_code": "conversion_error",
            },
        )
    )

    client = DatalatheClient(BASE)
    rs = client.generate_report_stream(["chip1"], ["SELECT * FROM t"])

    assert rs.next()
    assert rs.get_string("name") == "Alice"
    with pytest.raises(DatalatheQueryError) as excinfo:
        rs.next()
    assert excinfo.value.errors == {0: "Conversion Error: cast failed mid-scan"}


@responses.activate
def test_stream_truncated_no_terminal_frame_raises() -> None:
    _register(
        _ndjson(
            _SCHEMA,
            {"type": "rows", "rows": [["Alice", "30"]]},
        )
    )

    client = DatalatheClient(BASE)
    rs = client.generate_report_stream(["chip1"], ["SELECT * FROM t"])

    assert rs.next()
    assert rs.get_string("name") == "Alice"
    with pytest.raises(DatalatheApiError, match="without a terminal frame"):
        rs.next()


@responses.activate
def test_stream_error_frame_before_rows_raises() -> None:
    _register(
        _ndjson(
            _SCHEMA,
            {"type": "error", "error": "storage unavailable"},
        )
    )

    client = DatalatheClient(BASE)
    rs = client.generate_report_stream(["chip1"], ["SELECT * FROM t"])

    with pytest.raises(DatalatheQueryError) as excinfo:
        rs.next()
    assert excinfo.value.errors == {0: "storage unavailable"}


@responses.activate
def test_stream_sets_stream_field_in_body() -> None:
    _register(_ndjson(_SCHEMA, {"type": "end", "row_count": 0, "timing": {}}))

    client = DatalatheClient(BASE)
    client.generate_report_stream(["chip1"], ["SELECT * FROM t"])

    sent = json.loads(responses.calls[0].request.body)
    assert sent["stream"] is True
    assert sent["query_request"]["query"] == ["SELECT * FROM t"]


@responses.activate
def test_stream_http_error_raises_before_streaming() -> None:
    responses.add(
        responses.POST,
        f"{BASE}/lathe/report",
        json={"error": "bad request"},
        status=400,
    )

    client = DatalatheClient(BASE)
    with pytest.raises(DatalatheApiError) as excinfo:
        client.generate_report_stream(["chip1"], ["SELECT * FROM t"])
    assert excinfo.value.status_code == 400
