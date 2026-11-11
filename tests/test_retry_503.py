"""Tests for automatic retry of HTTP 503 responses that carry a short Retry-After.

The engine answers 503 with a Retry-After header when it rejects a request
before doing any work, so replay is safe for every method. A short wait means
a replacement engine is about to take over; a long one, or none, means the
caller decides. Behavior is exercised against a real local HTTP server because
mocked transports bypass the urllib3 retry machinery.
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from datalathe import DatalatheApiError, DatalatheClient

SHUTTING_DOWN_BODY = json.dumps({"error": "engine is shutting down", "error_code": "SHUTTING_DOWN"})


class _Engine:
    def __init__(self, statuses: tuple[int, ...], retry_after: str | None, fail_first: int):
        self.statuses = statuses
        self.retry_after = retry_after
        self.fail_first = fail_first
        self.request_count = 0
        self.url = ""


@pytest.fixture
def engine():
    servers: list[tuple[HTTPServer, threading.Thread]] = []

    def start(
        retry_after: str | None,
        fail_first: int = 100,
        statuses: tuple[int, ...] = (503,),
    ) -> _Engine:
        state = _Engine(statuses, retry_after, fail_first)

        class Handler(BaseHTTPRequestHandler):
            def _respond(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                if length:
                    self.rfile.read(length)
                state.request_count += 1
                if state.request_count <= state.fail_first:
                    body = SHUTTING_DOWN_BODY.encode()
                    self.send_response(state.statuses[(state.request_count - 1) % len(state.statuses)])
                    if state.retry_after is not None:
                        self.send_header("Retry-After", state.retry_after)
                else:
                    body = json.dumps([]).encode()
                    self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            do_GET = _respond
            do_POST = _respond

            def log_message(self, *args) -> None:
                pass

        server = HTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        servers.append((server, thread))
        state.url = f"http://127.0.0.1:{server.server_address[1]}"
        return state

    yield start
    for server, thread in servers:
        server.shutdown()
        server.server_close()
        thread.join()


def _assert_surfaces_503(excinfo: pytest.ExceptionInfo[DatalatheApiError]) -> None:
    assert type(excinfo.value) is DatalatheApiError
    assert excinfo.value.status_code == 503
    assert excinfo.value.response_body == SHUTTING_DOWN_BODY
    assert SHUTTING_DOWN_BODY in str(excinfo.value)


def test_retries_503_with_short_retry_after_until_success(engine) -> None:
    state = engine(retry_after="1", fail_first=1)
    client = DatalatheClient(state.url)
    assert client.get_databases() == []
    assert state.request_count == 2


def test_retries_503_at_the_retry_after_limit(engine, monkeypatch) -> None:
    sleeps: list[float] = []
    monkeypatch.setattr("urllib3.util.retry.time.sleep", sleeps.append)
    state = engine(retry_after="30", fail_first=1)
    client = DatalatheClient(state.url)
    assert client.get_databases() == []
    assert state.request_count == 2
    assert sleeps == [30]


def test_retries_503_for_post(engine) -> None:
    state = engine(retry_after="1", fail_first=1)
    client = DatalatheClient(state.url)
    client._post("/lathe/stage/data", {"sources": []})
    assert state.request_count == 2


@pytest.mark.parametrize(
    "retry_after",
    ["31", "60", "soon", "1.5", "-1", "", "Wed, 21 Oct 2037 07:28:00 GMT", None],
)
def test_503_outside_the_rule_surfaces_at_once(engine, retry_after: str | None) -> None:
    state = engine(retry_after=retry_after)
    client = DatalatheClient(state.url)
    with pytest.raises(DatalatheApiError) as excinfo:
        client.get_databases()
    _assert_surfaces_503(excinfo)
    assert state.request_count == 1


def test_retry_on_429_false_disables_503_retries(engine) -> None:
    state = engine(retry_after="1")
    client = DatalatheClient(state.url, retry_on_429=False)
    with pytest.raises(DatalatheApiError) as excinfo:
        client.get_databases()
    _assert_surfaces_503(excinfo)
    assert state.request_count == 1


def test_max_retries_zero_disables_503_retries(engine) -> None:
    state = engine(retry_after="1")
    client = DatalatheClient(state.url, max_retries=0)
    with pytest.raises(DatalatheApiError) as excinfo:
        client.get_databases()
    _assert_surfaces_503(excinfo)
    assert state.request_count == 1


def test_exhausted_503_retries_raise_datalathe_api_error(engine) -> None:
    state = engine(retry_after="1")
    client = DatalatheClient(state.url, max_retries=2)
    with pytest.raises(DatalatheApiError) as excinfo:
        client.get_databases()
    _assert_surfaces_503(excinfo)
    assert state.request_count == 3


def test_429_and_503_share_one_budget(engine) -> None:
    state = engine(retry_after="1", statuses=(429, 503, 503))
    client = DatalatheClient(state.url, max_retries=2)
    with pytest.raises(DatalatheApiError) as excinfo:
        client.get_databases()
    _assert_surfaces_503(excinfo)
    assert state.request_count == 3


def test_429_with_long_retry_after_is_still_retried(engine, monkeypatch) -> None:
    sleeps: list[float] = []
    monkeypatch.setattr("urllib3.util.retry.time.sleep", sleeps.append)
    state = engine(retry_after="60", fail_first=1, statuses=(429,))
    client = DatalatheClient(state.url)
    assert client.get_databases() == []
    assert state.request_count == 2
    assert sleeps == [60]


def test_other_5xx_with_retry_after_is_not_retried(engine) -> None:
    state = engine(retry_after="1", statuses=(500,))
    client = DatalatheClient(state.url)
    with pytest.raises(DatalatheApiError) as excinfo:
        client.get_databases()
    assert excinfo.value.status_code == 500
    assert state.request_count == 1
