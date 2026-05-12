"""Tests for the typed unreadable_chip_ids field on ChipsResponse.

v1.7.1+ engines include unreadable_chip_ids in the /lathe/chips response so
callers can identify chips whose metadata couldn't be read back. The client
surfaces it as a typed list[str] on ChipsResponse.
"""
from __future__ import annotations

import responses

from datalathe import DatalatheClient


BASE = "http://localhost:8080"


@responses.activate
def test_list_chips_surfaces_unreadable_chip_ids() -> None:
    responses.add(
        responses.GET,
        f"{BASE}/lathe/chips",
        json={
            "chips": [
                {
                    "chip_id": "good-1",
                    "sub_chip_id": "sub-1",
                    "table_name": "users",
                    "partition_value": "default",
                    "created_at": 1700000000,
                },
            ],
            "metadata": [],
            "unreadable_chip_ids": ["bad-1", "bad-2"],
        },
        status=200,
    )

    client = DatalatheClient(BASE)
    result = client.list_chips()

    assert len(result.chips) == 1
    assert result.chips[0].chip_id == "good-1"
    assert result.unreadable_chip_ids == ["bad-1", "bad-2"]


@responses.activate
def test_list_chips_defaults_unreadable_chip_ids_to_empty_list() -> None:
    """Older engines pre-v1.7.1 don't emit the field — clients still get an empty list."""
    responses.add(
        responses.GET,
        f"{BASE}/lathe/chips",
        json={"chips": [], "metadata": []},
        status=200,
    )

    client = DatalatheClient(BASE)
    result = client.list_chips()

    assert result.unreadable_chip_ids == []
