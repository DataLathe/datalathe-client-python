import http.server
import socketserver
import threading
import time

import pytest

from datalathe.client import (
    IDLE_CONNECTION_KEEP_ALIVE_SECONDS,
    DatalatheClient,
    _IdleExpiringHTTPAdapter,
)

SHORTEST_COMMON_PROXY_IDLE_TIMEOUT_SECONDS = 60


class _Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        body = b'{"chips":[],"metadata":[]}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


class _CountingServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, *args, **kwargs):
        self.connections = 0
        super().__init__(*args, **kwargs)

    def get_request(self):
        request = super().get_request()
        self.connections += 1
        return request


@pytest.fixture
def server():
    srv = _CountingServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield srv
    srv.shutdown()
    srv.server_close()


def _client(srv, max_idle_seconds):
    client = DatalatheClient(f"http://127.0.0.1:{srv.server_address[1]}")
    for adapter in client._session.adapters.values():
        adapter._max_idle_seconds = max_idle_seconds
    return client


def test_idle_connection_is_not_reused_after_the_bound(server):
    client = _client(server, max_idle_seconds=0.2)

    client.list_chips()
    after_first = server.connections
    time.sleep(0.5)
    client.list_chips()

    assert server.connections > after_first, (
        "a connection idle beyond the bound must be discarded; reusing it risks writing "
        "into a socket a proxy has already closed"
    )


def test_connection_is_reused_within_the_bound(server):
    client = _client(server, max_idle_seconds=30.0)

    client.list_chips()
    after_first = server.connections
    client.list_chips()

    assert server.connections == after_first, "back-to-back requests should reuse one connection"


def test_bound_is_below_common_proxy_idle_timeouts():
    assert IDLE_CONNECTION_KEEP_ALIVE_SECONDS < SHORTEST_COMMON_PROXY_IDLE_TIMEOUT_SECONDS


def test_adapter_is_mounted_even_without_retries():
    client = DatalatheClient("http://127.0.0.1:1", retry_on_429=False)

    assert all(
        isinstance(a, _IdleExpiringHTTPAdapter) for a in client._session.adapters.values()
    )
