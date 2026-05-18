"""Tests for surfacing per-query execution errors from generate_report.

The engine runs /lathe/report in with_errors mode: a query that fails at
DuckDB execution time comes back HTTP 200 with the per-entry `error` field
populated and `result` null. These tests lock the contract that the client
raises on that instead of silently returning empty results.
"""
from __future__ import annotations

import pytest
import responses

from datalathe import DatalatheClient, DatalatheQueryError


BASE = "http://localhost:8080"


@responses.activate
def test_generate_report_raises_on_failed_query() -> None:
    responses.add(
        responses.POST,
        f"{BASE}/lathe/report",
        json={
            "result": {
                "0": {
                    "idx": "0",
                    "result": None,
                    "error": "Binder Error: Referenced column \"foo\" not found",
                    "schema": None,
                }
            }
        },
        status=200,
    )

    client = DatalatheClient(BASE)
    with pytest.raises(DatalatheQueryError) as excinfo:
        client.generate_report(["chip1"], ["SELECT foo FROM t"])

    assert excinfo.value.errors == {0: "Binder Error: Referenced column \"foo\" not found"}


@responses.activate
def test_generate_report_opt_out_returns_entry_with_error() -> None:
    responses.add(
        responses.POST,
        f"{BASE}/lathe/report",
        json={
            "result": {
                "0": {"idx": "0", "result": None, "error": "boom", "schema": None}
            }
        },
        status=200,
    )

    client = DatalatheClient(BASE)
    report = client.generate_report(
        ["chip1"], ["SELECT 1"], raise_on_query_error=False
    )

    assert report.results[0].error == "boom"
    assert report.results[0].result is None


@responses.activate
def test_generate_report_does_not_raise_on_success() -> None:
    responses.add(
        responses.POST,
        f"{BASE}/lathe/report",
        json={
            "result": {
                "0": {
                    "idx": "0",
                    "result": [["1"]],
                    "error": None,
                    "schema": [{"name": "n", "data_type": "INTEGER"}],
                }
            }
        },
        status=200,
    )

    client = DatalatheClient(BASE)
    report = client.generate_report(["chip1"], ["SELECT 1"])

    assert report.results[0].result == [["1"]]


@responses.activate
def test_generate_report_reports_every_failed_query() -> None:
    responses.add(
        responses.POST,
        f"{BASE}/lathe/report",
        json={
            "result": {
                "0": {"idx": "0", "result": [["1"]], "error": None, "schema": None},
                "1": {"idx": "1", "result": None, "error": "bad query", "schema": None},
                "2": {"idx": "2", "result": None, "error": "worse query", "schema": None},
            }
        },
        status=200,
    )

    client = DatalatheClient(BASE)
    with pytest.raises(DatalatheQueryError) as excinfo:
        client.generate_report(
            ["chip1"], ["SELECT 1", "SELECT bad", "SELECT worse"]
        )

    assert excinfo.value.errors == {1: "bad query", 2: "worse query"}
