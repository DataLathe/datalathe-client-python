from __future__ import annotations

import logging

import responses

from datalathe import DatalatheClient

BASE = "http://localhost:8080"


@responses.activate
def test_copy_sends_header_original_does_not_and_shares_session():
    responses.add(responses.GET, f"{BASE}/lathe/jobs/j1", json={"job_id": "j1", "status": "queued"})
    responses.add(responses.GET, f"{BASE}/lathe/jobs/j1", json={"job_id": "j1", "status": "queued"})
    base = DatalatheClient(BASE)
    scoped = base.with_correlation_id("nc-1")
    assert scoped is not base
    assert scoped._session is base._session

    scoped.get_ingest_job("j1")
    base.get_ingest_job("j1")

    assert responses.calls[0].request.headers["X-Correlation-ID"] == "nc-1"
    assert "X-Correlation-ID" not in responses.calls[1].request.headers


def test_blank_or_none_returns_same_client():
    base = DatalatheClient(BASE)
    assert base.with_correlation_id(None) is base
    assert base.with_correlation_id("") is base
    assert base.with_correlation_id("   ") is base


@responses.activate
def test_invalid_id_warns_and_sends_nothing(caplog):
    responses.add(responses.GET, f"{BASE}/lathe/jobs/j1", json={"job_id": "j1", "status": "queued"})
    base = DatalatheClient(BASE)
    with caplog.at_level(logging.WARNING, logger="datalathe.client"):
        scoped = base.with_correlation_id("bad id\n")
    assert scoped is base
    assert "correlation ID" in caplog.text
    scoped.get_ingest_job("j1")
    assert "X-Correlation-ID" not in responses.calls[0].request.headers


@responses.activate
def test_chained_copy_uses_latest_id_on_post():
    responses.add(responses.POST, f"{BASE}/lathe/jobs/j1/resume", json={"job_id": "j1", "chip_id": "c"}, status=202)
    scoped = DatalatheClient(BASE).with_correlation_id("first").with_correlation_id("second")
    scoped.resume_ingest_job("j1")
    sent = responses.calls[0].request.headers
    assert sent["X-Correlation-ID"] == "second"
    assert sent["Content-Type"] == "application/json"


@responses.activate
def test_ingest_job_correlation_id_parses_and_tolerates_absence():
    responses.add(responses.GET, f"{BASE}/lathe/jobs/j1", json={"job_id": "j1", "status": "queued", "correlation_id": "nc-job"})
    responses.add(responses.GET, f"{BASE}/lathe/jobs/j2", json={"job_id": "j2", "status": "queued"})
    client = DatalatheClient(BASE)
    assert client.get_ingest_job("j1").correlation_id == "nc-job"
    assert client.get_ingest_job("j2").correlation_id is None


@responses.activate
def test_non_string_id_is_stringified_never_raises():
    import uuid

    responses.add(responses.GET, f"{BASE}/lathe/jobs/j1", json={"job_id": "j1", "status": "queued"})
    u = uuid.UUID("03a813fe-6a62-53fb-8144-846c197ac8f6")
    scoped = DatalatheClient(BASE).with_correlation_id(u)  # type: ignore[arg-type]
    scoped.get_ingest_job("j1")
    assert responses.calls[0].request.headers["X-Correlation-ID"] == str(u)
