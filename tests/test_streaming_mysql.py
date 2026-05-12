"""Tests for v1.7 streaming MySQL ingest fields.

Covers:
- SourceRequest.streaming and SourceRequest.partition_column serialization
- StageDataResponse.total_rows and StageDataResponse.elapsed_ms deserialization
- Omission of optional fields when None (clean wire shape)
"""
from __future__ import annotations

import json

import pytest
import responses

from datalathe import DatalatheClient
from datalathe.types import SourceRequest, StageDataResponse, _to_dict, _from_dict


BASE = "http://localhost:8080"


# ---------------------------------------------------------------------------
# SourceRequest serialization
# ---------------------------------------------------------------------------

def test_source_request_includes_streaming_and_partition_column_when_set() -> None:
    req = SourceRequest(
        database_name="mydb",
        query="SELECT * FROM orders",
        streaming=True,
        partition_column="id",
    )
    d = _to_dict(req)
    assert d["streaming"] is True
    assert d["partition_column"] == "id"


def test_source_request_omits_streaming_and_partition_column_when_none() -> None:
    req = SourceRequest(
        database_name="mydb",
        query="SELECT * FROM orders",
    )
    d = _to_dict(req)
    assert "streaming" not in d
    assert "partition_column" not in d


def test_source_request_streaming_false_is_included() -> None:
    req = SourceRequest(
        database_name="mydb",
        query="SELECT * FROM orders",
        streaming=False,
    )
    d = _to_dict(req)
    assert d["streaming"] is False


# ---------------------------------------------------------------------------
# StageDataResponse deserialization
# ---------------------------------------------------------------------------

def test_stage_data_response_parses_total_rows_and_elapsed_ms() -> None:
    payload = {
        "chip_id": "chip-abc",
        "total_rows": 200_000_000,
        "elapsed_ms": 1_843_121,
    }
    resp = _from_dict(StageDataResponse, payload)
    assert resp.chip_id == "chip-abc"
    assert resp.total_rows == 200_000_000
    assert resp.elapsed_ms == 1_843_121


def test_stage_data_response_fields_absent_when_not_in_wire_payload() -> None:
    payload = {"chip_id": "chip-xyz"}
    resp = _from_dict(StageDataResponse, payload)
    assert resp.total_rows is None
    assert resp.elapsed_ms is None


# ---------------------------------------------------------------------------
# End-to-end: create_chip sends streaming fields and receives full response
# ---------------------------------------------------------------------------

@responses.activate
def test_create_chip_sends_streaming_fields_on_wire() -> None:
    captured: list[dict] = []

    def _capture(request):  # type: ignore[no-untyped-def]
        captured.append(json.loads(request.body))
        return (200, {}, json.dumps({"chip_id": "chip-stream-1"}))

    responses.add_callback(
        responses.POST,
        f"{BASE}/lathe/stage/data",
        callback=_capture,
        content_type="application/json",
    )

    client = DatalatheClient(BASE)
    from datalathe.types import SourceRequest, SourceType
    chip_id = client.create_chips(
        sources=[SourceRequest(
            database_name="mydb",
            query="SELECT * FROM orders",
            streaming=True,
            partition_column="id",
        )],
        source_type=SourceType.MYSQL,
    )[0]

    assert chip_id == "chip-stream-1"
    assert len(captured) == 1
    src = captured[0]["source_request"]
    assert src["streaming"] is True
    assert src["partition_column"] == "id"


@responses.activate
def test_create_chip_omits_streaming_fields_when_not_set() -> None:
    captured: list[dict] = []

    def _capture(request):  # type: ignore[no-untyped-def]
        captured.append(json.loads(request.body))
        return (200, {}, json.dumps({"chip_id": "chip-buffered-1"}))

    responses.add_callback(
        responses.POST,
        f"{BASE}/lathe/stage/data",
        callback=_capture,
        content_type="application/json",
    )

    client = DatalatheClient(BASE)
    from datalathe.types import SourceRequest, SourceType
    client.create_chips(
        sources=[SourceRequest(database_name="mydb", query="SELECT * FROM orders")],
        source_type=SourceType.MYSQL,
    )

    src = captured[0]["source_request"]
    assert "streaming" not in src
    assert "partition_column" not in src
