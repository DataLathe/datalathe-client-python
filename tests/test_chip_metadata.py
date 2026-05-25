"""Tests for the typed partition_column field on ChipMetadata.

Partitioned chips record which column they were partitioned by. The
chip-manager returns it as partition_column in the /lathe/chips metadata;
the client surfaces it as a typed field on ChipMetadata.
"""
from __future__ import annotations

import responses

from datalathe import DatalatheClient


BASE = "http://localhost:8080"


@responses.activate
def test_chip_metadata_surfaces_partition_column() -> None:
    responses.add(
        responses.GET,
        f"{BASE}/lathe/chips",
        json={
            "chips": [],
            "metadata": [
                {
                    "chip_id": "chip1",
                    "created_at": 1700000000,
                    "description": "pruning-demo",
                    "name": "pruning-demo",
                    "partition_column": "country",
                },
            ],
        },
        status=200,
    )

    client = DatalatheClient(BASE)
    result = client.list_chips()

    assert result.metadata[0].partition_column == "country"


@responses.activate
def test_chip_metadata_partition_column_defaults_to_none() -> None:
    """Unpartitioned chips omit the field — clients still get None."""
    responses.add(
        responses.GET,
        f"{BASE}/lathe/chips",
        json={
            "chips": [],
            "metadata": [
                {
                    "chip_id": "chip1",
                    "created_at": 1700000000,
                    "description": "d",
                    "name": "n",
                },
            ],
        },
        status=200,
    )

    client = DatalatheClient(BASE)
    result = client.list_chips()

    assert result.metadata[0].partition_column is None
