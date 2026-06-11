"""Tests for v1.7.12 chip list pagination.

GET /lathe/chips accepts optional limit/offset query params and the response
carries total_count (total matching chips regardless of the page requested).
"""
from __future__ import annotations

import responses

from datalathe import DatalatheClient


BASE = "http://localhost:8080"

CHIPS_BODY = {
    "chips": [
        {
            "chip_id": "chip-1",
            "sub_chip_id": "sub-1",
            "table_name": "users",
            "partition_value": "default",
            "created_at": 1700000000,
        },
    ],
    "metadata": [],
    "unreadable_chip_ids": [],
    "total_count": 42,
}


@responses.activate
def test_list_chips_sends_limit_and_offset() -> None:
    responses.add(responses.GET, f"{BASE}/lathe/chips", json=CHIPS_BODY, status=200)

    client = DatalatheClient(BASE)
    result = client.list_chips(limit=10, offset=20)

    assert result.total_count == 42
    url = responses.calls[0].request.url
    assert "limit=10" in url
    assert "offset=20" in url


@responses.activate
def test_list_chips_default_omits_pagination_params() -> None:
    responses.add(responses.GET, f"{BASE}/lathe/chips", json=CHIPS_BODY, status=200)

    client = DatalatheClient(BASE)
    client.list_chips()

    assert responses.calls[0].request.url == f"{BASE}/lathe/chips"


@responses.activate
def test_list_chips_sends_limit_without_offset() -> None:
    responses.add(responses.GET, f"{BASE}/lathe/chips", json=CHIPS_BODY, status=200)

    client = DatalatheClient(BASE)
    client.list_chips(limit=5)

    url = responses.calls[0].request.url
    assert "limit=5" in url
    assert "offset" not in url


@responses.activate
def test_list_chips_parses_total_count() -> None:
    responses.add(responses.GET, f"{BASE}/lathe/chips", json=CHIPS_BODY, status=200)

    client = DatalatheClient(BASE)
    result = client.list_chips(limit=1)

    assert len(result.chips) == 1
    assert result.chips[0].chip_id == "chip-1"
    assert result.total_count == 42


@responses.activate
def test_total_count_defaults_to_none_on_older_engines() -> None:
    responses.add(
        responses.GET,
        f"{BASE}/lathe/chips",
        json={"chips": [], "metadata": []},
        status=200,
    )

    client = DatalatheClient(BASE)
    result = client.list_chips()

    assert result.total_count is None
