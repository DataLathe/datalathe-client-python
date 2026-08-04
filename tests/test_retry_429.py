"""Tests for automatic retry of HTTP 429 responses.

A 429 from the engine means the request was rejected before any work was done,
so replay is safe for every method. Wiring is asserted on the mounted adapter;
behavior is exercised against a real local HTTP server because mocked
transports bypass the urllib3 retry machinery.
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from datalathe import DatalatheApiError, DatalatheClient


def _mounted_retry(client: DatalatheClient, prefix: str = "https://"):
    return client._session.get_adapter(f"{prefix}example.com").max_retries


def test_session_mounts_429_retry_by_default() -> None:
    client = DatalatheClient("http://localhost:8080")
    for prefix in ("http://", "https://"):
        retry = _mounted_retry(client, prefix)
        assert retry.total is None
        assert retry.connect == 0
        assert retry.read == 0
        assert retry.redirect == 0
        assert retry.other == 0
        assert retry.status == 3
        assert retry.status_forcelist == [429]
        assert retry.allowed_methods is None
        assert retry.respect_retry_after_header is True
        assert retry.raise_on_status is False


def test_max_retries_kwarg_tunes_status_budget() -> None:
    client = DatalatheClient("http://localhost:8080", max_retries=7)
    assert _mounted_retry(client).status == 7


def test_retry_on_429_false_leaves_default_adapter() -> None:
    client = DatalatheClient("http://localhost:8080", retry_on_429=False)
    retry = _mounted_retry(client)
    assert retry.total == 0
    assert retry.status_forcelist != [429]


class _FlakyHandler(BaseHTTPRequestHandler):
    fail_first = 2
    request_count = 0

    def do_GET(self) -> None:
        cls = type(self)
        cls.request_count += 1
        if cls.request_count <= cls.fail_first:
            self.send_response(429)
            self.send_header("Retry-After", "0")
            self.send_header("Content-Length", "0")
            self.end_headers()
        else:
            body = json.dumps([]).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def log_message(self, *args) -> None:
        pass


@pytest.fixture
def flaky_server():
    _FlakyHandler.request_count = 0
    server = HTTPServer(("127.0.0.1", 0), _FlakyHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        thread.join()


def test_retries_429_until_success(flaky_server: str) -> None:
    client = DatalatheClient(flaky_server)
    assert client.get_databases() == []
    assert _FlakyHandler.request_count == 3


def test_disabled_retries_surface_429_immediately(flaky_server: str) -> None:
    client = DatalatheClient(flaky_server, retry_on_429=False)
    with pytest.raises(DatalatheApiError) as excinfo:
        client.get_databases()
    assert excinfo.value.status_code == 429
    assert _FlakyHandler.request_count == 1


def test_exhausted_retries_raise_datalathe_api_error() -> None:
    _FlakyHandler.request_count = 0
    _FlakyHandler.fail_first = 10
    server = HTTPServer(("127.0.0.1", 0), _FlakyHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        client = DatalatheClient(f"http://127.0.0.1:{server.server_address[1]}")
        with pytest.raises(DatalatheApiError) as excinfo:
            client.get_databases()
        assert excinfo.value.status_code == 429
        assert _FlakyHandler.request_count == 4
    finally:
        _FlakyHandler.fail_first = 2
        server.shutdown()
        thread.join()
