#!/usr/bin/env python3 -m pytest
from __future__ import annotations

from click.testing import CliRunner

from plextraktsync.cli import cli
from plextraktsync.commands import broker_client
from plextraktsync.watch.client_registry import BrokerClientRegistry


def registry(tmp_path):
    return BrokerClientRegistry(str(tmp_path / "watch-broker.sqlite3"))


def collect_output():
    lines = []

    def write(line):
        lines.append(str(line))

    return lines, write


def test_broker_client_create_prints_secret_once_and_not_hash(tmp_path):
    r = registry(tmp_path)
    lines, write = collect_output()

    broker_client.create(
        client_id="tina",
        plex_username="Tina",
        registry=r,
        print=write,
    )

    output = "\n".join(lines)
    client = r.get_client("tina")

    assert "Created broker client: tina" in output
    assert "Client secret, shown once" in output
    assert client.secret_hash not in output
    assert r.authenticate("tina", lines[-1]) == client


def test_broker_client_list_hides_revoked_by_default(tmp_path):
    r = registry(tmp_path)
    r.create_client(client_id="tina", plex_username="Tina")
    r.create_client(client_id="other", plex_username="Other")
    r.revoke_client("other")

    lines, write = collect_output()
    broker_client.list_clients(registry=r, print=write)

    output = "\n".join(lines)
    assert "tina" in output
    assert "active" in output
    assert "other" not in output
    assert "revoked" not in output


def test_broker_client_list_can_show_revoked_clients(tmp_path):
    r = registry(tmp_path)
    r.create_client(client_id="tina", plex_username="Tina")
    r.revoke_client("tina")

    lines, write = collect_output()
    broker_client.list_clients(show_revoked=True, registry=r, print=write)

    output = "\n".join(lines)
    assert "tina" in output
    assert "revoked" in output


def test_broker_client_revoke_prints_revoked_client(tmp_path):
    r = registry(tmp_path)
    r.create_client(client_id="tina", plex_username="Tina")
    lines, write = collect_output()

    broker_client.revoke("tina", registry=r, print=write)

    assert lines == ["Revoked broker client: tina"]
    assert r.get_client("tina") is None


def test_broker_client_cli_group_is_registered():
    result = CliRunner().invoke(cli, ["broker-client", "--help"])

    assert result.exit_code == 0
    assert "create" in result.output
    assert "list" in result.output
    assert "revoke" in result.output
