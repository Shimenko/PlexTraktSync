#!/usr/bin/env python3 -m pytest
from __future__ import annotations

import sqlite3

import pytest

from plextraktsync.watch.client_registry import (
    SECRET_HASH_PREFIX,
    SECRET_PREFIX,
    BrokerClientRegistry,
    hash_client_secret,
    verify_client_secret,
)


def registry(tmp_path):
    return BrokerClientRegistry(str(tmp_path / "watch-broker.sqlite3"))


def test_registry_migrate_creates_schema(tmp_path):
    r = registry(tmp_path)

    r.migrate()

    with sqlite3.connect(r.db_path) as conn:
        version = conn.execute("SELECT version FROM broker_schema").fetchone()[0]
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}

    assert version == 1
    assert "broker_clients" in tables


def test_create_client_returns_secret_and_stores_only_hash(tmp_path):
    r = registry(tmp_path)

    client, secret = r.create_client(client_id="tina", plex_username="Tina")

    assert secret.startswith(SECRET_PREFIX)
    assert client.secret_hash.startswith(SECRET_HASH_PREFIX)
    assert secret not in client.secret_hash
    assert r.authenticate("tina", secret) == client
    assert r.authenticate("tina", "wrong") is None


def test_secret_hash_verification_rejects_unknown_hash_format():
    secret = "ptsc_secret"

    assert verify_client_secret(secret, hash_client_secret(secret))
    assert not verify_client_secret(secret, "plain-hash-value")


def test_create_client_rejects_invalid_client_id(tmp_path):
    r = registry(tmp_path)

    with pytest.raises(ValueError, match="client_id must match"):
        r.create_client(client_id="bad/id", plex_username="Tina")


def test_create_client_rejects_duplicate_client_id(tmp_path):
    r = registry(tmp_path)
    r.create_client(client_id="tina", plex_username="Tina")

    with pytest.raises(ValueError, match="Broker client already exists"):
        r.create_client(client_id="tina", plex_username="Other")


def test_create_client_rejects_duplicate_active_plex_username(tmp_path):
    r = registry(tmp_path)
    r.create_client(client_id="tina", plex_username="Tina")

    with pytest.raises(ValueError, match="Active broker client already exists for Plex username"):
        r.create_client(client_id="tina-laptop", plex_username="Tina")


def test_create_client_rejects_duplicate_active_plex_account_id(tmp_path):
    r = registry(tmp_path)
    r.create_client(client_id="tina", plex_username="Tina", plex_account_id="123")

    with pytest.raises(ValueError, match="Active broker client already exists for Plex account ID"):
        r.create_client(client_id="other", plex_username="Other", plex_account_id="123")


def test_revoke_client_disables_authentication_and_hides_from_default_list(tmp_path):
    r = registry(tmp_path)
    client, secret = r.create_client(client_id="tina", plex_username="Tina")

    revoked = r.revoke_client(client.client_id)

    assert revoked.revoked_at is not None
    assert r.authenticate("tina", secret) is None
    assert r.list_clients() == []
    assert r.list_clients(include_revoked=True) == [revoked]


def test_revoke_client_keeps_plex_username_reusable(tmp_path):
    r = registry(tmp_path)
    r.create_client(client_id="tina", plex_username="Tina")
    r.revoke_client("tina")

    client, _ = r.create_client(client_id="tina-new", plex_username="Tina")

    assert client.client_id == "tina-new"
    assert client.plex_username == "Tina"


def test_revoke_client_rejects_missing_or_revoked_client(tmp_path):
    r = registry(tmp_path)

    with pytest.raises(ValueError, match="No active broker client found"):
        r.revoke_client("missing")

    r.create_client(client_id="tina", plex_username="Tina")
    r.revoke_client("tina")

    with pytest.raises(ValueError, match="No active broker client found"):
        r.revoke_client("tina")
