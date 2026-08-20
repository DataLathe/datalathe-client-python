"""Tests for ChipResolver freshness-tag eviction."""

from __future__ import annotations

import json

import pytest
import responses

from datalathe import ChipResolver, DatalatheClient, TableDef

BASE = "http://localhost:8080"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _chips_response(
    chips: list[dict[str, str]],
    tags: list[dict[str, str]] | None = None,
) -> dict[str, list[dict[str, str]]]:
    return {
        "chips": chips,
        "metadata": [],
        "tags": tags or [],
    }


def _chip(chip_id: str, table: str, pv: str = "") -> dict[str, str]:
    return {
        "chip_id": chip_id,
        "sub_chip_id": chip_id,
        "table_name": table,
        "partition_value": pv,
    }


def _tag(chip_id: str, key: str, value: str) -> dict[str, str]:
    return {"chip_id": chip_id, "key": key, "value": value}


def _add_search(chips: list[dict[str, str]],
                tags: list[dict[str, str]] | None = None) -> None:
    responses.add(
        responses.GET,
        f"{BASE}/lathe/chips/search",
        json=_chips_response(chips, tags),
    )


def _add_delete(chip_id: str, status: int = 200,
                body: dict[str, str] | None = None) -> None:
    responses.add(
        responses.DELETE,
        f"{BASE}/lathe/chips/{chip_id}",
        json=body if body is not None else {},
        status=status,
    )


def _add_stage(chip_id: str) -> None:
    responses.add(
        responses.POST,
        f"{BASE}/lathe/stage/data",
        json={"chip_id": chip_id, "error": None},
    )


USERS_V2 = TableDef(
    "users", "select * from users", tenant_field="org_id",
    freshness_tags={"schema_version": "v2"},
)
ORDERS_V2 = TableDef(
    "orders", "select * from orders",
    partitioned=True, partition_field="order_date", tenant_field="org_id",
    freshness_tags={"schema_version": "v2"},
)
CATEGORIES_DEF = TableDef(
    "categories", "select * from categories",
)


def _resolver(*table_defs: TableDef) -> ChipResolver:
    return ChipResolver(DatalatheClient(BASE), table_defs=list(table_defs))


# ---------------------------------------------------------------------------
# Eviction on resolve_chips
# ---------------------------------------------------------------------------

@responses.activate
def test_stale_chip_evicted_and_recreated_in_one_pass() -> None:
    _add_search(
        [_chip("c1", "users")],
        [_tag("c1", "tenant", "42"), _tag("c1", "schema_version", "v1")],
    )
    _add_delete("c1")
    _add_stage("c2")

    ids = _resolver(USERS_V2).resolve_chips(["users"], [], "42")

    assert ids == ["c2"]
    assert len(responses.calls) == 3
    assert responses.calls[1].request.method == "DELETE"
    assert responses.calls[1].request.url.endswith("/lathe/chips/c1")
    assert responses.calls[2].request.method == "POST"


@responses.activate
def test_matching_freshness_tags_keep_chip() -> None:
    _add_search(
        [_chip("c1", "users")],
        [_tag("c1", "tenant", "42"), _tag("c1", "schema_version", "v2")],
    )

    ids = _resolver(USERS_V2).resolve_chips(["users"], [], "42")

    assert ids == ["c1"]
    assert len(responses.calls) == 1


@responses.activate
def test_untagged_chip_with_declared_freshness_evicted() -> None:
    _add_search([_chip("c1", "users")], [_tag("c1", "tenant", "42")])
    _add_delete("c1")
    _add_stage("c2")

    ids = _resolver(USERS_V2).resolve_chips(["users"], [], "42")

    assert ids == ["c2"]
    assert len(responses.calls) == 3


@responses.activate
def test_no_freshness_tags_leave_chips_alone() -> None:
    users_plain = TableDef(
        "users", "select * from users", tenant_field="org_id",
    )
    _add_search([_chip("c1", "users")], [_tag("c1", "tenant", "42")])

    ids = _resolver(users_plain).resolve_chips(["users"], [], "42")

    assert ids == ["c1"]
    assert len(responses.calls) == 1


@responses.activate
def test_any_differing_tag_among_several_evicts() -> None:
    users_two_tags = TableDef(
        "users", "select * from users", tenant_field="org_id",
        freshness_tags={
            "schema_version": "v2", "latest_max_date": "2026-08-19",
        },
    )
    _add_search(
        [_chip("c1", "users")],
        [
            _tag("c1", "tenant", "42"),
            _tag("c1", "schema_version", "v2"),
            _tag("c1", "latest_max_date", "2026-07-31"),
        ],
    )
    _add_delete("c1")
    _add_stage("c2")

    ids = _resolver(users_two_tags).resolve_chips(["users"], [], "42")

    assert ids == ["c2"]
    assert len(responses.calls) == 3


# ---------------------------------------------------------------------------
# Freshness tags on created chips
# ---------------------------------------------------------------------------

@responses.activate
def test_created_chips_carry_merged_freshness_tags() -> None:
    _add_search([])
    _add_stage("c_cat")
    _add_stage("c_users")

    ids = _resolver(USERS_V2, CATEGORIES_DEF).resolve_chips(
        ["users", "categories"], [], "42",
    )

    assert sorted(ids) == ["c_cat", "c_users"]
    cat_body = json.loads(responses.calls[1].request.body)
    assert cat_body["source_request"]["table_name"] == "categories"
    assert cat_body["tags"] == {"tenant": "42"}
    users_body = json.loads(responses.calls[2].request.body)
    assert users_body["source_request"]["table_name"] == "users"
    assert users_body["tags"] == {"tenant": "42", "schema_version": "v2"}


@responses.activate
def test_callable_freshness_tags_evaluated_per_resolve() -> None:
    users_callable = TableDef(
        "users", "select * from users", tenant_field="org_id",
        freshness_tags=lambda: {"load_date": "2026-08-20"},
    )
    _add_search(
        [_chip("c1", "users")],
        [_tag("c1", "tenant", "42"), _tag("c1", "load_date", "2026-08-19")],
    )
    _add_delete("c1")
    _add_stage("c2")

    ids = _resolver(users_callable).resolve_chips(["users"], [], "42")

    assert ids == ["c2"]
    create_body = json.loads(responses.calls[2].request.body)
    assert create_body["tags"] == {"tenant": "42", "load_date": "2026-08-20"}


# ---------------------------------------------------------------------------
# Eviction failure modes
# ---------------------------------------------------------------------------

@responses.activate
def test_delete_404_still_recreates() -> None:
    _add_search(
        [_chip("c1", "users")],
        [_tag("c1", "tenant", "42"), _tag("c1", "schema_version", "v1")],
    )
    _add_delete("c1", status=404, body={
        "error_code": "chip_not_found", "chip_id": "c1", "error": "gone",
    })
    _add_stage("c2")

    ids = _resolver(USERS_V2).resolve_chips(["users"], [], "42")

    assert ids == ["c2"]
    assert len(responses.calls) == 3


@responses.activate
def test_delete_failure_keeps_stale_chip() -> None:
    _add_search(
        [_chip("c1", "users")],
        [_tag("c1", "tenant", "42"), _tag("c1", "schema_version", "v1")],
    )
    _add_delete("c1", status=500, body={"error": "boom"})

    ids = _resolver(USERS_V2).resolve_chips(["users"], [], "42")

    assert ids == ["c1"]
    assert len(responses.calls) == 2


# ---------------------------------------------------------------------------
# Partitioned tables
# ---------------------------------------------------------------------------

@responses.activate
def test_only_stale_partition_recreated() -> None:
    _add_search(
        [
            _chip("c1", "orders", "2024-01-31"),
            _chip("c2", "orders", "2024-02-29"),
        ],
        [
            _tag("c1", "tenant", "42"), _tag("c1", "schema_version", "v1"),
            _tag("c2", "tenant", "42"), _tag("c2", "schema_version", "v2"),
        ],
    )
    _add_delete("c1")
    _add_stage("c3")

    ids = _resolver(ORDERS_V2).resolve_chips(
        ["orders"], ["2024-01-31", "2024-02-29"], "42",
    )

    assert sorted(ids) == ["c2", "c3"]
    assert len(responses.calls) == 3
    assert responses.calls[1].request.method == "DELETE"
    assert responses.calls[1].request.url.endswith("/lathe/chips/c1")
    create_body = json.loads(responses.calls[2].request.body)
    partition = create_body["source_request"]["partition"]
    assert partition["partition_values"] == ["2024-01-31"]
    assert create_body["tags"] == {"tenant": "42", "schema_version": "v2"}


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

@responses.activate
def test_freshness_key_colliding_with_tag_key_raises() -> None:
    users_colliding = TableDef(
        "users", "select * from users", tenant_field="org_id",
        freshness_tags={"tenant": "43"},
    )

    with pytest.raises(ValueError, match="collides"):
        _resolver(users_colliding).resolve_chips(["users"], [], "42")
    assert len(responses.calls) == 0


# ---------------------------------------------------------------------------
# Global chips
# ---------------------------------------------------------------------------

@responses.activate
def test_warm_global_chips_evicts_stale_global_chip() -> None:
    categories_v2 = TableDef(
        "categories", "select * from categories",
        freshness_tags={"schema_version": "v2"},
    )
    _add_search(
        [_chip("c_old", "categories")],
        [
            _tag("c_old", "tenant", "global"),
            _tag("c_old", "schema_version", "v1"),
        ],
    )
    _add_delete("c_old")
    _add_stage("c_new")

    resolver = _resolver(categories_v2)
    created = resolver.warm_global_chips()

    assert created == ["c_new"]
    assert responses.calls[1].request.method == "DELETE"
    assert responses.calls[1].request.url.endswith("/lathe/chips/c_old")
    create_body = json.loads(responses.calls[2].request.body)
    assert create_body["tags"] == {
        "tenant": "global", "schema_version": "v2",
    }
    assert resolver.resolve_chips(["categories"], [], "42") == ["c_new"]
    assert len(responses.calls) == 3
