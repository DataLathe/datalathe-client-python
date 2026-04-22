"""Tests for ChipResolver — chip resolution and high-level query pipeline."""

from __future__ import annotations

import json
from typing import Any

import pytest
import responses

from datalathe import (
    ChipNotFoundError,
    ChipResolver,
    DatalatheApiError,
    DatalatheClient,
    TableDef,
)

BASE = "http://localhost:8080"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _chips_response(
    chips: list[dict[str, str]],
) -> dict[str, list[dict[str, str]]]:
    return {
        "chips": chips,
        "metadata": [],
        "tags": [],
    }


def _stage_response(chip_id: str) -> dict[str, str | None]:
    return {"chip_id": chip_id, "error": None}


def _report_response(
    rows: list[list[str | None]],
    schema: list[dict[str, str]],
) -> dict[str, Any]:
    return {
        "result": {
            "0": {
                "idx": "0",
                "result": rows,
                "schema": schema,
            },
        },
        "timing": {
            "total_ms": 10.0,
            "chip_attach_ms": 2.0,
            "query_execution_ms": 8.0,
        },
    }


def _extract_response(
    tables: list[str],
    transformed_query: str = "SELECT ...",
) -> dict[str, Any]:
    return {"tables": tables, "transformed_query": transformed_query}


USERS_DEF = TableDef(
    "users", "select * from users", tenant_field="org_id",
)
ORDERS_DEF = TableDef(
    "orders", "select * from orders",
    partitioned=True, partition_field="order_date", tenant_field="org_id",
)
CATEGORIES_DEF = TableDef(
    "categories", "select * from categories",
)


# ---------------------------------------------------------------------------
# TableDef validation
# ---------------------------------------------------------------------------

def test_table_def_partitioned_requires_partition_field() -> None:
    with pytest.raises(ValueError, match="partition_field is required"):
        TableDef("broken", "select * from broken", partitioned=True)


def test_table_def_rejects_where_in_sql() -> None:
    with pytest.raises(ValueError, match="must not contain a WHERE"):
        TableDef("bad", "select * from bad WHERE active = 1")


# ---------------------------------------------------------------------------
# resolve_chips — all existing
# ---------------------------------------------------------------------------

@responses.activate
def test_resolve_chips_all_existing() -> None:
    responses.add(
        responses.GET,
        f"{BASE}/lathe/chips/search",
        json=_chips_response([
            {"chip_id": "c_users", "sub_chip_id": "c_users",
             "table_name": "users", "partition_value": ""},
        ]),
    )

    resolver = ChipResolver(
        DatalatheClient(BASE), table_defs=[USERS_DEF],
    )
    ids = resolver.resolve_chips(["users"], [], "42")

    assert ids == ["c_users"]
    assert len(responses.calls) == 1


# ---------------------------------------------------------------------------
# resolve_chips — all missing, unpartitioned
# ---------------------------------------------------------------------------

@responses.activate
def test_resolve_chips_creates_missing_unpartitioned() -> None:
    responses.add(
        responses.GET,
        f"{BASE}/lathe/chips/search",
        json=_chips_response([]),
    )
    responses.add(
        responses.POST,
        f"{BASE}/lathe/stage/data",
        json=_stage_response("c_users_new"),
    )

    resolver = ChipResolver(
        DatalatheClient(BASE), table_defs=[USERS_DEF],
    )
    ids = resolver.resolve_chips(["users"], [], "42")

    assert ids == ["c_users_new"]
    stage_body = json.loads(responses.calls[1].request.body)
    assert stage_body["source_request"]["table_name"] == "users"
    assert "WHERE org_id = '42'" in stage_body["source_request"]["query"]
    assert stage_body["tags"] == {"tenant": "42"}


# ---------------------------------------------------------------------------
# resolve_chips — all missing, partitioned
# ---------------------------------------------------------------------------

@responses.activate
def test_resolve_chips_creates_missing_partitioned() -> None:
    responses.add(
        responses.GET,
        f"{BASE}/lathe/chips/search",
        json=_chips_response([]),
    )
    responses.add(
        responses.POST,
        f"{BASE}/lathe/stage/data",
        json=_stage_response("c_orders_jan"),
    )

    resolver = ChipResolver(
        DatalatheClient(BASE), table_defs=[ORDERS_DEF],
    )
    ids = resolver.resolve_chips(["orders"], ["2024-01-31"], "42")

    assert ids == ["c_orders_jan"]
    stage_body = json.loads(responses.calls[1].request.body)
    assert "WHERE org_id = '42' AND order_date = '2024-01-31'" in stage_body["source_request"]["query"]
    assert stage_body["source_request"]["partition"]["partition_by"] == "order_date"
    assert stage_body["source_request"]["partition"]["partition_values"] == ["2024-01-31"]


# ---------------------------------------------------------------------------
# resolve_chips — mixed existing and missing
# ---------------------------------------------------------------------------

@responses.activate
def test_resolve_chips_mixed_existing_and_missing() -> None:
    responses.add(
        responses.GET,
        f"{BASE}/lathe/chips/search",
        json=_chips_response([
            {"chip_id": "c_users", "sub_chip_id": "c_users",
             "table_name": "users", "partition_value": ""},
        ]),
    )
    responses.add(
        responses.POST,
        f"{BASE}/lathe/stage/data",
        json=_stage_response("c_orders_new"),
    )

    resolver = ChipResolver(
        DatalatheClient(BASE), table_defs=[USERS_DEF, ORDERS_DEF],
    )
    ids = resolver.resolve_chips(
        ["users", "orders"], ["2024-01-31"], "42",
    )

    assert "c_users" in ids
    assert "c_orders_new" in ids
    assert len(ids) == 2


# ---------------------------------------------------------------------------
# resolve_chips — force_recreate skips search
# ---------------------------------------------------------------------------

@responses.activate
def test_resolve_chips_force_recreate_skips_search() -> None:
    responses.add(
        responses.POST,
        f"{BASE}/lathe/stage/data",
        json=_stage_response("c_users_fresh"),
    )

    resolver = ChipResolver(
        DatalatheClient(BASE), table_defs=[USERS_DEF],
    )
    ids = resolver.resolve_chips(
        ["users"], [], "42", force_recreate=True,
    )

    assert ids == ["c_users_fresh"]
    assert all(c.request.method == "POST" for c in responses.calls)


# ---------------------------------------------------------------------------
# resolve_chips — tenant_field produces WHERE clause
# ---------------------------------------------------------------------------

@responses.activate
def test_resolve_chips_tenant_where_clause() -> None:
    responses.add(
        responses.GET,
        f"{BASE}/lathe/chips/search",
        json=_chips_response([]),
    )
    responses.add(
        responses.POST,
        f"{BASE}/lathe/stage/data",
        json=_stage_response("c_users"),
    )

    resolver = ChipResolver(
        DatalatheClient(BASE), table_defs=[USERS_DEF],
    )
    resolver.resolve_chips(["users"], [], "tenant-99")

    stage_body = json.loads(responses.calls[1].request.body)
    assert "WHERE org_id = 'tenant-99'" in stage_body["source_request"]["query"]


# ---------------------------------------------------------------------------
# resolve_chips — no tenant_field omits WHERE clause
# ---------------------------------------------------------------------------

@responses.activate
def test_resolve_chips_no_tenant_field() -> None:
    responses.add(
        responses.GET,
        f"{BASE}/lathe/chips/search",
        json=_chips_response([]),
    )
    responses.add(
        responses.POST,
        f"{BASE}/lathe/stage/data",
        json=_stage_response("c_cat"),
    )

    resolver = ChipResolver(
        DatalatheClient(BASE), table_defs=[CATEGORIES_DEF],
    )
    resolver.resolve_chips(["categories"], [], "42")

    stage_body = json.loads(responses.calls[1].request.body)
    assert stage_body["source_request"]["query"] == "select * from categories"


# ---------------------------------------------------------------------------
# resolve_chips — unregistered table raises
# ---------------------------------------------------------------------------

def test_resolve_chips_unregistered_table_raises() -> None:
    resolver = ChipResolver(DatalatheClient(BASE))
    with pytest.raises(ValueError, match="No TableDef registered"):
        resolver.resolve_chips(["mystery_table"], [], "42")


# ---------------------------------------------------------------------------
# resolve_chips — root chip deduplication
# ---------------------------------------------------------------------------

@responses.activate
def test_resolve_chips_ignores_sub_chips_for_unpartitioned() -> None:
    """Only root chips (chip_id == sub_chip_id) match unpartitioned tables."""
    responses.add(
        responses.GET,
        f"{BASE}/lathe/chips/search",
        json=_chips_response([
            {"chip_id": "root", "sub_chip_id": "sub1",
             "table_name": "users", "partition_value": ""},
            {"chip_id": "root2", "sub_chip_id": "root2",
             "table_name": "users", "partition_value": ""},
        ]),
    )

    resolver = ChipResolver(
        DatalatheClient(BASE), table_defs=[USERS_DEF],
    )
    ids = resolver.resolve_chips(["users"], [], "42")

    assert ids == ["root2"]


# ---------------------------------------------------------------------------
# resolve_chips — add_table
# ---------------------------------------------------------------------------

@responses.activate
def test_add_table() -> None:
    responses.add(
        responses.GET,
        f"{BASE}/lathe/chips/search",
        json=_chips_response([]),
    )
    responses.add(
        responses.POST,
        f"{BASE}/lathe/stage/data",
        json=_stage_response("c_added"),
    )

    resolver = ChipResolver(DatalatheClient(BASE))
    resolver.add_table(USERS_DEF)
    ids = resolver.resolve_chips(["users"], [], "42")

    assert ids == ["c_added"]
    stage_body = json.loads(responses.calls[1].request.body)
    assert "WHERE org_id = '42'" in stage_body["source_request"]["query"]


# ---------------------------------------------------------------------------
# resolve_chips — custom tag_key
# ---------------------------------------------------------------------------

@responses.activate
def test_custom_tag_key() -> None:
    responses.add(
        responses.GET,
        f"{BASE}/lathe/chips/search",
        json=_chips_response([]),
    )
    responses.add(
        responses.POST,
        f"{BASE}/lathe/stage/data",
        json=_stage_response("c1"),
    )

    resolver = ChipResolver(
        DatalatheClient(BASE),
        table_defs=[CATEGORIES_DEF],
        tag_key="env",
    )
    resolver.resolve_chips(["categories"], [], "prod")

    search_url = responses.calls[0].request.url
    assert "tag=env%3Aprod" in search_url
    stage_body = json.loads(responses.calls[1].request.body)
    assert stage_body["tags"] == {"env": "prod"}


# ---------------------------------------------------------------------------
# resolve_chips — validation errors
# ---------------------------------------------------------------------------

def test_resolve_chips_empty_tables_raises() -> None:
    resolver = ChipResolver(DatalatheClient(BASE))
    with pytest.raises(ValueError, match="tables must not be empty"):
        resolver.resolve_chips([], [], "42")


def test_resolve_chips_invalid_tenant_id_raises() -> None:
    resolver = ChipResolver(
        DatalatheClient(BASE), table_defs=[USERS_DEF],
    )
    with pytest.raises(ValueError, match="Invalid tenant_id"):
        resolver.resolve_chips(["users"], [], "'; DROP TABLE --")


def test_resolve_chips_invalid_partition_value_raises() -> None:
    resolver = ChipResolver(
        DatalatheClient(BASE), table_defs=[USERS_DEF],
    )
    with pytest.raises(ValueError, match="Invalid partition_value"):
        resolver.resolve_chips(["users"], ["<script>"], "42")


def test_resolve_chips_invalid_table_name_raises() -> None:
    td = TableDef("valid_name", "select * from valid_name")
    resolver = ChipResolver(DatalatheClient(BASE), table_defs=[td])
    with pytest.raises(ValueError, match="Invalid table name"):
        resolver.resolve_chips(["valid_name; DROP TABLE x"], [], "42")


# ---------------------------------------------------------------------------
# resolve_chips — partitioned table with empty partition_values
# ---------------------------------------------------------------------------

@responses.activate
def test_resolve_chips_partitioned_with_no_partition_values() -> None:
    """Partitioned table with empty partition_values produces no chips for it."""
    responses.add(
        responses.GET,
        f"{BASE}/lathe/chips/search",
        json=_chips_response([]),
    )
    responses.add(
        responses.POST,
        f"{BASE}/lathe/stage/data",
        json=_stage_response("c_users"),
    )

    resolver = ChipResolver(
        DatalatheClient(BASE), table_defs=[USERS_DEF, ORDERS_DEF],
    )
    ids = resolver.resolve_chips(["users", "orders"], [], "42")

    assert ids == ["c_users"]
    assert len(responses.calls) == 2


# ---------------------------------------------------------------------------
# query — end-to-end
# ---------------------------------------------------------------------------

@responses.activate
def test_query_end_to_end() -> None:
    responses.add(
        responses.POST,
        f"{BASE}/lathe/query/tables",
        json=_extract_response(["users"], "SELECT transformed"),
    )
    responses.add(
        responses.GET,
        f"{BASE}/lathe/chips/search",
        json=_chips_response([
            {"chip_id": "c_users", "sub_chip_id": "c_users",
             "table_name": "users", "partition_value": ""},
        ]),
    )
    responses.add(
        responses.POST,
        f"{BASE}/lathe/report",
        json=_report_response(
            [["Alice", "30"]], [
                {"name": "name", "data_type": "Utf8"},
                {"name": "age", "data_type": "Int32"},
            ],
        ),
    )

    resolver = ChipResolver(
        DatalatheClient(BASE), table_defs=[USERS_DEF],
    )
    result = resolver.query("SELECT name, age FROM users", tenant_id="42")

    assert 0 in result.results
    assert result.results[0].result == [["Alice", "30"]]


# ---------------------------------------------------------------------------
# query — retry on ChipNotFoundError
# ---------------------------------------------------------------------------

@responses.activate
def test_query_retries_on_chip_not_found() -> None:
    responses.add(
        responses.POST,
        f"{BASE}/lathe/query/tables",
        json=_extract_response(["users"], "SELECT transformed"),
    )
    responses.add(
        responses.GET,
        f"{BASE}/lathe/chips/search",
        json=_chips_response([
            {"chip_id": "c_stale", "sub_chip_id": "c_stale",
             "table_name": "users", "partition_value": ""},
        ]),
    )
    # First report fails — chip expired
    responses.add(
        responses.POST,
        f"{BASE}/lathe/report",
        json={
            "error": "Chip expired",
            "error_code": "chip_not_found",
            "chip_id": "c_stale",
        },
        status=404,
    )
    # Retry: force_recreate creates new chip
    responses.add(
        responses.POST,
        f"{BASE}/lathe/stage/data",
        json=_stage_response("c_fresh"),
    )
    # Retry report succeeds
    responses.add(
        responses.POST,
        f"{BASE}/lathe/report",
        json=_report_response(
            [["Bob"]], [{"name": "name", "data_type": "Utf8"}],
        ),
    )

    resolver = ChipResolver(
        DatalatheClient(BASE), table_defs=[USERS_DEF],
    )
    result = resolver.query("SELECT name FROM users", tenant_id="42")

    assert result.results[0].result == [["Bob"]]
    report_calls = [
        c for c in responses.calls if "/lathe/report" in c.request.url
    ]
    assert len(report_calls) == 2


# ---------------------------------------------------------------------------
# query — no retry when disabled
# ---------------------------------------------------------------------------

@responses.activate
def test_query_propagates_chip_not_found_when_retry_disabled() -> None:
    responses.add(
        responses.POST,
        f"{BASE}/lathe/query/tables",
        json=_extract_response(["users"], "SELECT transformed"),
    )
    responses.add(
        responses.GET,
        f"{BASE}/lathe/chips/search",
        json=_chips_response([
            {"chip_id": "c_stale", "sub_chip_id": "c_stale",
             "table_name": "users", "partition_value": ""},
        ]),
    )
    responses.add(
        responses.POST,
        f"{BASE}/lathe/report",
        json={
            "error": "Chip expired",
            "error_code": "chip_not_found",
            "chip_id": "c_stale",
        },
        status=404,
    )

    resolver = ChipResolver(
        DatalatheClient(BASE), table_defs=[USERS_DEF],
    )
    with pytest.raises(ChipNotFoundError):
        resolver.query(
            "SELECT 1 FROM users",
            tenant_id="42",
            retry_on_expired=False,
        )


# ---------------------------------------------------------------------------
# query — non-ChipNotFoundError propagates
# ---------------------------------------------------------------------------

@responses.activate
def test_query_propagates_other_api_errors() -> None:
    responses.add(
        responses.POST,
        f"{BASE}/lathe/query/tables",
        json=_extract_response(["users"], "SELECT transformed"),
    )
    responses.add(
        responses.GET,
        f"{BASE}/lathe/chips/search",
        json=_chips_response([
            {"chip_id": "c1", "sub_chip_id": "c1",
             "table_name": "users", "partition_value": ""},
        ]),
    )
    responses.add(
        responses.POST,
        f"{BASE}/lathe/report",
        body="Internal Server Error",
        status=500,
    )

    resolver = ChipResolver(
        DatalatheClient(BASE), table_defs=[USERS_DEF],
    )
    with pytest.raises(DatalatheApiError) as exc_info:
        resolver.query("SELECT 1 FROM users", tenant_id="42")

    assert not isinstance(exc_info.value, ChipNotFoundError)
    assert exc_info.value.status_code == 500


# ---------------------------------------------------------------------------
# query — retry failure propagates
# ---------------------------------------------------------------------------

@responses.activate
def test_query_retry_failure_propagates() -> None:
    responses.add(
        responses.POST,
        f"{BASE}/lathe/query/tables",
        json=_extract_response(["users"], "SELECT transformed"),
    )
    responses.add(
        responses.GET,
        f"{BASE}/lathe/chips/search",
        json=_chips_response([
            {"chip_id": "c_stale", "sub_chip_id": "c_stale",
             "table_name": "users", "partition_value": ""},
        ]),
    )
    # First report: chip expired
    responses.add(
        responses.POST,
        f"{BASE}/lathe/report",
        json={
            "error": "Chip expired",
            "error_code": "chip_not_found",
            "chip_id": "c_stale",
        },
        status=404,
    )
    # Retry: create fresh chip
    responses.add(
        responses.POST,
        f"{BASE}/lathe/stage/data",
        json=_stage_response("c_fresh"),
    )
    # Retry report also fails
    responses.add(
        responses.POST,
        f"{BASE}/lathe/report",
        body="Internal Server Error",
        status=500,
    )

    resolver = ChipResolver(
        DatalatheClient(BASE), table_defs=[USERS_DEF],
    )
    with pytest.raises(DatalatheApiError) as exc_info:
        resolver.query("SELECT 1 FROM users", tenant_id="42")

    assert exc_info.value.status_code == 500


# ---------------------------------------------------------------------------
# query — extract_tables returning no tables raises ValueError
# ---------------------------------------------------------------------------

@responses.activate
def test_query_raises_on_no_tables() -> None:
    responses.add(
        responses.POST,
        f"{BASE}/lathe/query/tables",
        json=_extract_response([]),
    )

    resolver = ChipResolver(DatalatheClient(BASE))
    with pytest.raises(ValueError, match="returned no tables"):
        resolver.query("SELECT 1", tenant_id="42")


# ---------------------------------------------------------------------------
# query — extract_tables API error propagates
# ---------------------------------------------------------------------------

@responses.activate
def test_query_propagates_extract_tables_error() -> None:
    responses.add(
        responses.POST,
        f"{BASE}/lathe/query/tables",
        json={"error": "parse failure"},
        status=400,
    )

    resolver = ChipResolver(
        DatalatheClient(BASE), table_defs=[USERS_DEF],
    )
    with pytest.raises(DatalatheApiError) as exc_info:
        resolver.query("INVALID SQL", tenant_id="42")

    assert exc_info.value.status_code == 400
