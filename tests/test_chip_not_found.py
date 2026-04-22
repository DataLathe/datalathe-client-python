"""Tests for the ChipNotFoundError detection in the HTTP layer.

Locks the wire-format contract with the engine:
HTTP 404 + body `{error_code: "chip_not_found", chip_id: ...}` → ChipNotFoundError.
"""
from __future__ import annotations

import json

import pytest
import responses

from datalathe import ChipNotFoundError, DatalatheApiError, DatalatheClient


BASE = "http://localhost:8080"


@responses.activate
def test_post_raises_chip_not_found_error_on_structured_404() -> None:
    responses.add(
        responses.POST,
        f"{BASE}/lathe/report",
        json={
            "error": "Chip 'abc123' is not available (may have expired)",
            "error_code": "chip_not_found",
            "chip_id": "abc123",
        },
        status=404,
    )

    client = DatalatheClient(BASE)
    with pytest.raises(ChipNotFoundError) as excinfo:
        client.generate_report(["abc123"], ["SELECT 1"])

    assert excinfo.value.chip_id == "abc123"
    assert excinfo.value.status_code == 404
    # Back-compat: still catchable as the parent type.
    assert isinstance(excinfo.value, DatalatheApiError)


@responses.activate
def test_get_raises_chip_not_found_error_on_structured_404() -> None:
    responses.add(
        responses.GET,
        f"{BASE}/lathe/chips/missing",
        json={
            "error": "Chip 'missing' is not available (may have expired)",
            "error_code": "chip_not_found",
            "chip_id": "missing",
        },
        status=404,
    )

    client = DatalatheClient(BASE)
    with pytest.raises(ChipNotFoundError) as excinfo:
        client.get_chip("missing")

    assert excinfo.value.chip_id == "missing"


@responses.activate
def test_falls_back_to_generic_api_error_on_unstructured_404() -> None:
    responses.add(
        responses.POST,
        f"{BASE}/lathe/report",
        body="Not Found",
        status=404,
    )

    client = DatalatheClient(BASE)
    with pytest.raises(DatalatheApiError) as excinfo:
        client.generate_report(["x"], ["SELECT 1"])

    assert not isinstance(excinfo.value, ChipNotFoundError)
    assert excinfo.value.status_code == 404


@responses.activate
def test_falls_back_to_generic_api_error_when_error_code_missing() -> None:
    responses.add(
        responses.POST,
        f"{BASE}/lathe/report",
        json={"error": "Some other 404 reason"},
        status=404,
    )

    client = DatalatheClient(BASE)
    with pytest.raises(DatalatheApiError) as excinfo:
        client.generate_report(["x"], ["SELECT 1"])

    assert not isinstance(excinfo.value, ChipNotFoundError)


@responses.activate
def test_500_does_not_match_chip_not_found_path() -> None:
    """Defense in depth: only 404 should be inspected for the typed code."""
    responses.add(
        responses.POST,
        f"{BASE}/lathe/report",
        json={"error_code": "chip_not_found", "chip_id": "abc"},
        status=500,
    )

    client = DatalatheClient(BASE)
    with pytest.raises(DatalatheApiError) as excinfo:
        client.generate_report(["abc"], ["SELECT 1"])

    assert not isinstance(excinfo.value, ChipNotFoundError)
    assert excinfo.value.status_code == 500
