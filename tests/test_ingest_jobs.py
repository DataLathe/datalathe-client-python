"""Tests for v1.7.12 async ingest jobs.

Covers:
- create_chip_async submits the stage request with async: true and parses
  the 202 job handle
- create_chip stays byte-identical (no async key on the default path)
- get_ingest_job / list_ingest_jobs / resume_ingest_job wire shapes
- wait_for_ingest polling: success, failure, cancellation, timeout
"""
from __future__ import annotations

import json

import pytest
import responses

from datalathe import (
    DatalatheClient,
    DatalatheIngestError,
    DatalatheIngestTimeoutError,
    IngestJob,
)


BASE = "http://localhost:8080"
JOB_ID = "job-123"


def _job_body(status: str, **extra) -> dict:
    body = {
        "job_id": JOB_ID,
        "chip_id": "chip-abc",
        "status": status,
        "created_at": 1700000000,
        "updated_at": 1700000100,
        "heartbeat_at": 1700000100,
        "request": "{}",
    }
    body.update(extra)
    return body


# ---------------------------------------------------------------------------
# Submit
# ---------------------------------------------------------------------------

@responses.activate
def test_create_chip_async_sends_async_flag_and_returns_job_handle() -> None:
    captured: list[dict] = []

    def _capture(request):  # type: ignore[no-untyped-def]
        captured.append(json.loads(request.body))
        return (202, {}, json.dumps({"job_id": JOB_ID, "chip_id": "chip-abc"}))

    responses.add_callback(
        responses.POST,
        f"{BASE}/lathe/stage/data",
        callback=_capture,
        content_type="application/json",
    )

    client = DatalatheClient(BASE)
    job = client.create_chip_async(
        "mydb",
        "SELECT * FROM orders",
        "orders",
        streaming=True,
        keyset_column="id",
    )

    assert isinstance(job, IngestJob)
    assert job.job_id == JOB_ID
    assert job.chip_id == "chip-abc"
    assert job.status is None

    body = captured[0]
    assert body["async"] is True
    assert body["source_type"] == "MYSQL"
    src = body["source_request"]
    assert src["database_name"] == "mydb"
    assert src["table_name"] == "orders"
    assert src["streaming"] is True
    assert src["keyset_column"] == "id"


@responses.activate
def test_create_chip_default_path_has_no_async_key() -> None:
    captured: list[dict] = []

    def _capture(request):  # type: ignore[no-untyped-def]
        captured.append(json.loads(request.body))
        return (200, {}, json.dumps({"chip_id": "chip-sync"}))

    responses.add_callback(
        responses.POST,
        f"{BASE}/lathe/stage/data",
        callback=_capture,
        content_type="application/json",
    )

    client = DatalatheClient(BASE)
    chip_id = client.create_chip("mydb", "SELECT * FROM orders", "orders")

    assert chip_id == "chip-sync"
    assert "async" not in captured[0]


# ---------------------------------------------------------------------------
# Poll / list / resume
# ---------------------------------------------------------------------------

@responses.activate
def test_get_ingest_job_parses_full_record() -> None:
    responses.add(
        responses.GET,
        f"{BASE}/lathe/jobs/{JOB_ID}",
        json=_job_body("running", rows_ingested=500, chunks_done=2, chunks_total=8),
        status=200,
    )

    client = DatalatheClient(BASE)
    job = client.get_ingest_job(JOB_ID)

    assert job.job_id == JOB_ID
    assert job.chip_id == "chip-abc"
    assert job.status == "running"
    assert job.rows_ingested == 500
    assert job.chunks_done == 2
    assert job.chunks_total == 8
    assert job.error is None
    assert job.created_at == 1700000000
    assert job.updated_at == 1700000100


@responses.activate
def test_list_ingest_jobs_without_filter() -> None:
    responses.add(
        responses.GET,
        f"{BASE}/lathe/jobs",
        json=[_job_body("queued"), _job_body("succeeded")],
        status=200,
    )

    client = DatalatheClient(BASE)
    jobs = client.list_ingest_jobs()

    assert [j.status for j in jobs] == ["queued", "succeeded"]
    assert responses.calls[0].request.url == f"{BASE}/lathe/jobs"


@responses.activate
def test_list_ingest_jobs_sends_status_filter() -> None:
    responses.add(
        responses.GET,
        f"{BASE}/lathe/jobs",
        json=[_job_body("failed", error="boom")],
        status=200,
    )

    client = DatalatheClient(BASE)
    jobs = client.list_ingest_jobs(status="failed")

    assert len(jobs) == 1
    assert jobs[0].error == "boom"
    assert "status=failed" in responses.calls[0].request.url


@responses.activate
def test_resume_ingest_job_posts_and_returns_handle() -> None:
    responses.add(
        responses.POST,
        f"{BASE}/lathe/jobs/{JOB_ID}/resume",
        json={"job_id": JOB_ID, "chip_id": "chip-abc"},
        status=202,
    )

    client = DatalatheClient(BASE)
    job = client.resume_ingest_job(JOB_ID)

    assert job.job_id == JOB_ID
    assert job.chip_id == "chip-abc"


# ---------------------------------------------------------------------------
# wait_for_ingest
# ---------------------------------------------------------------------------

@responses.activate
def test_wait_for_ingest_polls_to_success() -> None:
    url = f"{BASE}/lathe/jobs/{JOB_ID}"
    responses.add(responses.GET, url, json=_job_body("queued"), status=200)
    responses.add(responses.GET, url, json=_job_body("running"), status=200)
    responses.add(
        responses.GET,
        url,
        json=_job_body("succeeded", rows_ingested=1000, chunks_done=8, chunks_total=8),
        status=200,
    )

    client = DatalatheClient(BASE)
    job = client.wait_for_ingest(JOB_ID, poll_interval=0.01, timeout=5.0)

    assert job.status == "succeeded"
    assert job.rows_ingested == 1000
    assert len(responses.calls) == 3


@responses.activate
def test_wait_for_ingest_raises_on_failed_with_job_error() -> None:
    url = f"{BASE}/lathe/jobs/{JOB_ID}"
    responses.add(responses.GET, url, json=_job_body("running"), status=200)
    responses.add(
        responses.GET,
        url,
        json=_job_body("failed", error="Lost connection during query"),
        status=200,
    )

    client = DatalatheClient(BASE)
    with pytest.raises(DatalatheIngestError) as exc_info:
        client.wait_for_ingest(JOB_ID, poll_interval=0.01, timeout=5.0)

    assert exc_info.value.job.status == "failed"
    assert exc_info.value.job.error == "Lost connection during query"
    assert "Lost connection during query" in str(exc_info.value)


@responses.activate
def test_wait_for_ingest_raises_on_cancelled() -> None:
    responses.add(
        responses.GET,
        f"{BASE}/lathe/jobs/{JOB_ID}",
        json=_job_body("cancelled"),
        status=200,
    )

    client = DatalatheClient(BASE)
    with pytest.raises(DatalatheIngestError) as exc_info:
        client.wait_for_ingest(JOB_ID, poll_interval=0.01, timeout=5.0)

    assert exc_info.value.job.status == "cancelled"


@responses.activate
def test_wait_for_ingest_raises_on_timeout() -> None:
    responses.add(
        responses.GET,
        f"{BASE}/lathe/jobs/{JOB_ID}",
        json=_job_body("queued"),
        status=200,
    )

    client = DatalatheClient(BASE)
    with pytest.raises(DatalatheIngestTimeoutError) as exc_info:
        client.wait_for_ingest(JOB_ID, poll_interval=0.01, timeout=0.0)

    assert exc_info.value.job.status == "queued"
    assert "queued" in str(exc_info.value)
