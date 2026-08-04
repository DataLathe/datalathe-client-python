"""Tests for the AI credential, context, and session management surface."""
from __future__ import annotations

import json

import pytest
import responses

from datalathe import DatalatheApiError, DatalatheClient


BASE = "http://localhost:8080"

CREDENTIAL = {
    "credential_id": "cred-1",
    "name": "prod-anthropic",
    "provider": "anthropic",
    "default_model": "some-model",
    "created_at": 1750000000,
    "tenant_id": "",
    "region": "",
}

CONTEXT = {
    "context_id": "ctx-1",
    "name": "loans",
    "chip_ids": "[\"chip-1\",\"chip-2\"]",
    "column_descriptions": "{\"loans\":{\"amount\":\"Loan amount in USD\"}}",
    "data_relationship_prompt": "loans joins borrowers on borrower_id",
    "created_at": 1750000000,
}


@responses.activate
def test_register_ai_credential_sends_body() -> None:
    responses.add(
        responses.POST, f"{BASE}/lathe/ai/credentials", json=CREDENTIAL, status=200
    )

    client = DatalatheClient(BASE)
    cred = client.register_ai_credential(
        name="prod-anthropic",
        provider="anthropic",
        api_key="sk-secret",
        default_model="some-model",
    )

    body = json.loads(responses.calls[0].request.body)
    assert body == {
        "name": "prod-anthropic",
        "provider": "anthropic",
        "api_key": "sk-secret",
        "default_model": "some-model",
    }
    assert cred.credential_id == "cred-1"
    assert cred.provider == "anthropic"
    assert cred.created_at == 1750000000


@responses.activate
def test_register_ai_credential_includes_region_when_given() -> None:
    payload = dict(CREDENTIAL, provider="bedrock", region="us-east-1")
    responses.add(
        responses.POST, f"{BASE}/lathe/ai/credentials", json=payload, status=200
    )

    client = DatalatheClient(BASE)
    cred = client.register_ai_credential(
        name="prod-bedrock",
        provider="bedrock",
        api_key="key",
        default_model="some-model",
        region="us-east-1",
    )

    body = json.loads(responses.calls[0].request.body)
    assert body["region"] == "us-east-1"
    assert cred.region == "us-east-1"


@responses.activate
def test_list_ai_credentials() -> None:
    responses.add(
        responses.GET, f"{BASE}/lathe/ai/credentials", json=[CREDENTIAL], status=200
    )

    client = DatalatheClient(BASE)
    creds = client.list_ai_credentials()

    assert len(creds) == 1
    assert creds[0].credential_id == "cred-1"
    assert creds[0].default_model == "some-model"


@responses.activate
def test_delete_ai_credential() -> None:
    responses.add(
        responses.DELETE, f"{BASE}/lathe/ai/credentials/cred-1", status=204
    )

    client = DatalatheClient(BASE)
    client.delete_ai_credential("cred-1")

    assert len(responses.calls) == 1


@responses.activate
def test_register_ai_context_sends_body() -> None:
    responses.add(
        responses.POST, f"{BASE}/lathe/ai/contexts", json=CONTEXT, status=200
    )

    client = DatalatheClient(BASE)
    ctx = client.register_ai_context(
        name="loans",
        chip_ids=["chip-1", "chip-2"],
        column_descriptions={"loans": {"amount": "Loan amount in USD"}},
        data_relationship_prompt="loans joins borrowers on borrower_id",
    )

    body = json.loads(responses.calls[0].request.body)
    assert body == {
        "name": "loans",
        "chip_ids": ["chip-1", "chip-2"],
        "column_descriptions": {"loans": {"amount": "Loan amount in USD"}},
        "data_relationship_prompt": "loans joins borrowers on borrower_id",
    }
    assert ctx.context_id == "ctx-1"
    assert json.loads(ctx.chip_ids) == ["chip-1", "chip-2"]
    assert json.loads(ctx.column_descriptions) == {
        "loans": {"amount": "Loan amount in USD"}
    }


@responses.activate
def test_list_ai_contexts() -> None:
    responses.add(
        responses.GET, f"{BASE}/lathe/ai/contexts", json=[CONTEXT], status=200
    )

    client = DatalatheClient(BASE)
    contexts = client.list_ai_contexts()

    assert len(contexts) == 1
    assert contexts[0].name == "loans"


@responses.activate
def test_get_ai_context() -> None:
    responses.add(
        responses.GET, f"{BASE}/lathe/ai/contexts/ctx-1", json=CONTEXT, status=200
    )

    client = DatalatheClient(BASE)
    ctx = client.get_ai_context("ctx-1")

    assert ctx.context_id == "ctx-1"
    assert ctx.data_relationship_prompt == "loans joins borrowers on borrower_id"


@responses.activate
def test_update_ai_context_sends_only_provided_fields() -> None:
    updated = dict(CONTEXT, name="loans-v2")
    responses.add(
        responses.PUT, f"{BASE}/lathe/ai/contexts/ctx-1", json=updated, status=200
    )

    client = DatalatheClient(BASE)
    ctx = client.update_ai_context("ctx-1", name="loans-v2")

    body = json.loads(responses.calls[0].request.body)
    assert body == {"name": "loans-v2"}
    assert ctx.name == "loans-v2"


@responses.activate
def test_update_ai_context_full_update() -> None:
    responses.add(
        responses.PUT, f"{BASE}/lathe/ai/contexts/ctx-1", json=CONTEXT, status=200
    )

    client = DatalatheClient(BASE)
    client.update_ai_context(
        "ctx-1",
        name="loans",
        chip_ids=["chip-1"],
        column_descriptions={"loans": {"amount": "USD"}},
        data_relationship_prompt="prompt",
    )

    body = json.loads(responses.calls[0].request.body)
    assert body == {
        "name": "loans",
        "chip_ids": ["chip-1"],
        "column_descriptions": {"loans": {"amount": "USD"}},
        "data_relationship_prompt": "prompt",
    }


@responses.activate
def test_delete_ai_context() -> None:
    responses.add(responses.DELETE, f"{BASE}/lathe/ai/contexts/ctx-1", status=204)

    client = DatalatheClient(BASE)
    client.delete_ai_context("ctx-1")

    assert len(responses.calls) == 1


@responses.activate
def test_delete_ai_session() -> None:
    responses.add(responses.DELETE, f"{BASE}/lathe/ai/sessions/sess-1", status=204)

    client = DatalatheClient(BASE)
    client.delete_ai_session("sess-1")

    assert len(responses.calls) == 1


@responses.activate
def test_ai_surface_error_mapping() -> None:
    responses.add(
        responses.GET,
        f"{BASE}/lathe/ai/contexts/missing",
        json={"error": "Context not found"},
        status=404,
    )

    client = DatalatheClient(BASE)
    with pytest.raises(DatalatheApiError) as excinfo:
        client.get_ai_context("missing")

    assert excinfo.value.status_code == 404
