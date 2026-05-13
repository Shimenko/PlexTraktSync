from __future__ import annotations

from plextraktsync.factory import factory


def create(
    client_id: str,
    plex_username: str,
    plex_account_id: str | None = None,
    registry=None,
    print=None,
):
    registry = registry or factory.broker_client_registry
    print = print or factory.print

    client, secret = registry.create_client(
        client_id=client_id,
        plex_username=plex_username,
        plex_account_id=plex_account_id,
    )

    print(f"Created broker client: {client.client_id}")
    print(f"Plex username: {client.plex_username}")
    if client.plex_account_id:
        print(f"Plex account ID: {client.plex_account_id}")
    print("Client secret, shown once. Store this on the client side now.")
    print(secret)


def list_clients(show_revoked: bool = False, registry=None, print=None):
    registry = registry or factory.broker_client_registry
    print = print or factory.print

    clients = registry.list_clients(include_revoked=show_revoked)
    if not clients:
        print("No broker clients found")
        return

    rows = [
        [
            client.client_id,
            client.plex_username,
            client.plex_account_id or "",
            "active" if client.active else "revoked",
            client.created_at,
        ]
        for client in clients
    ]
    _print_table(
        headers=["client_id", "plex_username", "plex_account_id", "status", "created_at"],
        rows=rows,
        print=print,
    )


def revoke(client_id: str, registry=None, print=None):
    registry = registry or factory.broker_client_registry
    print = print or factory.print

    client = registry.revoke_client(client_id)
    print(f"Revoked broker client: {client.client_id}")


def _print_table(headers: list[str], rows: list[list[str]], print):
    widths = [len(header) for header in headers]
    for row in rows:
        widths = [max(width, len(value)) for width, value in zip(widths, row, strict=True)]

    print(_format_row(headers, widths))
    print(_format_row(["-" * width for width in widths], widths))
    for row in rows:
        print(_format_row(row, widths))


def _format_row(values: list[str], widths: list[int]):
    return "  ".join(value.ljust(width) for value, width in zip(values, widths, strict=True))
