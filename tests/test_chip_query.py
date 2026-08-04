"""Tests for query_chips (POST /lathe/chips/query)."""
from __future__ import annotations

import json

import pytest
import responses

from datalathe import ChipNotFoundError, DatalatheApiError, DatalatheClient


BASE = "http://localhost:8080"


@responses.activate
def test_query_chips_sends_body_and_parses_result() -> None:
    responses.add(
        responses.POST,
        f"{BASE}/lathe/chips/query",
        json={
            "columns": [{"name": "n", "data_type": "BigInt"}],
            "rows": [["3"]],
            "truncated": False,
        },
        status=200,
    )

    client = DatalatheClient(BASE)
    result = client.query_chips(
        ["chip-1"], "SELECT COUNT(*) AS n FROM s_chip_1.main.loans"
    )

    body = json.loads(responses.calls[0].request.body)
    assert body == {
        "chip_ids": ["chip-1"],
        "query": "SELECT COUNT(*) AS n FROM s_chip_1.main.loans",
    }

    assert len(result.columns) == 1
    assert result.columns[0].name == "n"
    assert result.columns[0].data_type == "BigInt"
    assert result.rows == [["3"]]
    assert result.truncated is False


@responses.activate
def test_query_chips_multiple_chips_and_truncation() -> None:
    responses.add(
        responses.POST,
        f"{BASE}/lathe/chips/query",
        json={
            "columns": [
                {"name": "id", "data_type": "Integer"},
                {"name": "name", "data_type": "Varchar"},
            ],
            "rows": [["1", "alpha"], ["2", None]],
            "truncated": True,
        },
        status=200,
    )

    client = DatalatheClient(BASE)
    result = client.query_chips(["chip-1", "chip-2"], "SELECT id, name FROM t")

    body = json.loads(responses.calls[0].request.body)
    assert body["chip_ids"] == ["chip-1", "chip-2"]
    assert result.rows == [["1", "alpha"], ["2", None]]
    assert result.truncated is True


@responses.activate
def test_query_chips_raises_chip_not_found() -> None:
    responses.add(
        responses.POST,
        f"{BASE}/lathe/chips/query",
        json={
            "error": "Chip 'ghost' is not available (may have expired)",
            "error_code": "chip_not_found",
            "chip_id": "ghost",
        },
        status=404,
    )

    client = DatalatheClient(BASE)
    with pytest.raises(ChipNotFoundError) as excinfo:
        client.query_chips(["ghost"], "select 1")

    assert excinfo.value.chip_id == "ghost"
    assert excinfo.value.status_code == 404


@responses.activate
def test_query_chips_raises_api_error_on_sql_failure() -> None:
    responses.add(
        responses.POST,
        f"{BASE}/lathe/chips/query",
        json={"error": "Query failed: table t does not exist"},
        status=422,
    )

    client = DatalatheClient(BASE)
    with pytest.raises(DatalatheApiError) as excinfo:
        client.query_chips(["chip-1"], "SELECT * FROM t")

    assert not isinstance(excinfo.value, ChipNotFoundError)
    assert excinfo.value.status_code == 422
