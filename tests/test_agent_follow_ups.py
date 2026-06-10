"""Tests for the agent follow-ups surface.

Locks the wire-format contract with the engine: `agent_options` may carry an
optional `suggest_follow_ups` boolean (omitted means server-side default), and
the response's `follow_ups` array is omitted entirely when empty — the client
must default it to an empty list.
"""
from __future__ import annotations

import json

import responses

from datalathe import AgentOptions, DatalatheClient


BASE = "http://localhost:8080"
AGENT_URL = f"{BASE}/lathe/ai/agent"


@responses.activate
def test_agent_response_parses_follow_ups() -> None:
    responses.add(
        responses.POST,
        AGENT_URL,
        json={
            "request_id": "req-1",
            "answer": "Total revenue is $1.2M.",
            "follow_ups": [
                "How does this compare to last quarter?",
                "Which region contributed the most?",
            ],
        },
        status=200,
    )

    client = DatalatheClient(BASE)
    result = client.query_agent("ctx-1", "What is total revenue?")

    assert result.follow_ups == [
        "How does this compare to last quarter?",
        "Which region contributed the most?",
    ]


@responses.activate
def test_agent_response_without_follow_ups_defaults_to_empty_list() -> None:
    responses.add(
        responses.POST,
        AGENT_URL,
        json={"request_id": "req-1", "answer": "Total revenue is $1.2M."},
        status=200,
    )

    client = DatalatheClient(BASE)
    result = client.query_agent("ctx-1", "What is total revenue?")

    assert result.follow_ups == []


@responses.activate
def test_suggest_follow_ups_false_serializes_into_request() -> None:
    responses.add(
        responses.POST,
        AGENT_URL,
        json={"request_id": "req-1"},
        status=200,
    )

    client = DatalatheClient(BASE)
    client.query_agent(
        "ctx-1",
        "What is total revenue?",
        agent_options=AgentOptions(suggest_follow_ups=False),
    )

    sent = json.loads(responses.calls[0].request.body)
    assert sent["agent_options"] == {"suggest_follow_ups": False}


@responses.activate
def test_omitted_suggest_follow_ups_absent_from_request() -> None:
    responses.add(
        responses.POST,
        AGENT_URL,
        json={"request_id": "req-1"},
        status=200,
    )

    client = DatalatheClient(BASE)
    client.query_agent(
        "ctx-1",
        "What is total revenue?",
        agent_options=AgentOptions(max_iterations=5),
    )

    sent = json.loads(responses.calls[0].request.body)
    assert sent["agent_options"] == {"max_iterations": 5}
    assert "suggest_follow_ups" not in sent["agent_options"]


@responses.activate
def test_no_agent_options_omits_key_from_request() -> None:
    responses.add(
        responses.POST,
        AGENT_URL,
        json={"request_id": "req-1"},
        status=200,
    )

    client = DatalatheClient(BASE)
    client.query_agent("ctx-1", "What is total revenue?")

    sent = json.loads(responses.calls[0].request.body)
    assert "agent_options" not in sent
