from __future__ import annotations

import hashlib
import hmac
import re
import secrets
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from os.path import join

from plextraktsync.path import config_dir

CLIENT_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
SECRET_PREFIX = "ptsc_"
SECRET_BYTES = 48
SECRET_HASH_PREFIX = "sha256:"


@dataclass(frozen=True)
class BrokerClient:
    client_id: str
    plex_username: str
    plex_account_id: str | None
    secret_hash: str
    created_at: str
    revoked_at: str | None

    @property
    def active(self):
        return self.revoked_at is None


class BrokerClientRegistry:
    def __init__(self, db_path: str | None = None):
        self.db_path = db_path or join(config_dir, "watch-broker.sqlite3")

    def connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def migrate(self):
        with self.connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS broker_schema (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                )
                """,
            )
            version = conn.execute("SELECT COALESCE(MAX(version), 0) FROM broker_schema").fetchone()[0]
            if version < 1:
                self._migrate_1(conn)

    def _migrate_1(self, conn: sqlite3.Connection):
        conn.execute(
            """
            CREATE TABLE broker_clients (
                client_id TEXT PRIMARY KEY,
                plex_username TEXT NOT NULL,
                plex_account_id TEXT,
                secret_hash TEXT NOT NULL,
                created_at TEXT NOT NULL,
                revoked_at TEXT
            )
            """,
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX broker_clients_active_plex_username
            ON broker_clients(plex_username)
            WHERE revoked_at IS NULL
            """,
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX broker_clients_active_plex_account_id
            ON broker_clients(plex_account_id)
            WHERE revoked_at IS NULL AND plex_account_id IS NOT NULL
            """,
        )
        conn.execute(
            "INSERT INTO broker_schema(version, applied_at) VALUES (?, ?)",
            (1, utc_now()),
        )

    def create_client(self, client_id: str, plex_username: str, plex_account_id: str | None = None):
        validate_client_id(client_id)
        if not plex_username:
            raise ValueError("plex_username is required")

        self.migrate()
        secret = generate_client_secret()
        client = BrokerClient(
            client_id=client_id,
            plex_username=plex_username,
            plex_account_id=plex_account_id,
            secret_hash=hash_client_secret(secret),
            created_at=utc_now(),
            revoked_at=None,
        )

        with self.connect() as conn:
            if self.get_client(client_id, include_revoked=True, conn=conn):
                raise ValueError(f"Broker client already exists: {client_id}")
            if self.get_active_client_by_plex_username(plex_username, conn=conn):
                raise ValueError(f"Active broker client already exists for Plex username: {plex_username}")
            if plex_account_id and self.get_active_client_by_plex_account_id(plex_account_id, conn=conn):
                raise ValueError(f"Active broker client already exists for Plex account ID: {plex_account_id}")

            conn.execute(
                """
                INSERT INTO broker_clients(client_id, plex_username, plex_account_id, secret_hash, created_at, revoked_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    client.client_id,
                    client.plex_username,
                    client.plex_account_id,
                    client.secret_hash,
                    client.created_at,
                    client.revoked_at,
                ),
            )

        return client, secret

    def get_client(self, client_id: str, include_revoked=False, conn: sqlite3.Connection | None = None):
        query = "SELECT * FROM broker_clients WHERE client_id = ?"
        params: tuple[str, ...] = (client_id,)
        if not include_revoked:
            query += " AND revoked_at IS NULL"

        if conn is not None:
            row = conn.execute(query, params).fetchone()
            return self._row_to_client(row)

        self.migrate()
        with self.connect() as owned_conn:
            row = owned_conn.execute(query, params).fetchone()
            return self._row_to_client(row)

    def get_active_client_by_plex_username(self, plex_username: str, conn: sqlite3.Connection | None = None):
        query = "SELECT * FROM broker_clients WHERE plex_username = ? AND revoked_at IS NULL"
        return self._fetch_one(query, (plex_username,), conn=conn)

    def get_active_client_by_plex_account_id(self, plex_account_id: str, conn: sqlite3.Connection | None = None):
        query = "SELECT * FROM broker_clients WHERE plex_account_id = ? AND revoked_at IS NULL"
        return self._fetch_one(query, (plex_account_id,), conn=conn)

    def list_clients(self, include_revoked=False):
        self.migrate()
        query = "SELECT * FROM broker_clients"
        if not include_revoked:
            query += " WHERE revoked_at IS NULL"
        query += " ORDER BY client_id"

        with self.connect() as conn:
            return [self._row_to_client(row) for row in conn.execute(query)]

    def revoke_client(self, client_id: str):
        self.migrate()
        revoked_at = utc_now()
        with self.connect() as conn:
            client = self.get_client(client_id, conn=conn)
            if client is None:
                raise ValueError(f"No active broker client found: {client_id}")

            conn.execute(
                "UPDATE broker_clients SET revoked_at = ? WHERE client_id = ?",
                (revoked_at, client_id),
            )

        return BrokerClient(
            client_id=client.client_id,
            plex_username=client.plex_username,
            plex_account_id=client.plex_account_id,
            secret_hash=client.secret_hash,
            created_at=client.created_at,
            revoked_at=revoked_at,
        )

    def authenticate(self, client_id: str, client_secret: str):
        client = self.get_client(client_id)
        if client is None:
            return None
        if not verify_client_secret(client_secret, client.secret_hash):
            return None

        return client

    def _fetch_one(self, query: str, params: tuple[str, ...], conn: sqlite3.Connection | None = None):
        if conn is not None:
            return self._row_to_client(conn.execute(query, params).fetchone())

        self.migrate()
        with self.connect() as owned_conn:
            return self._row_to_client(owned_conn.execute(query, params).fetchone())

    @staticmethod
    def _row_to_client(row):
        if row is None:
            return None

        return BrokerClient(
            client_id=row["client_id"],
            plex_username=row["plex_username"],
            plex_account_id=row["plex_account_id"],
            secret_hash=row["secret_hash"],
            created_at=row["created_at"],
            revoked_at=row["revoked_at"],
        )


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def validate_client_id(client_id: str):
    if not CLIENT_ID_PATTERN.fullmatch(client_id or ""):
        raise ValueError("client_id must match ^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")


def generate_client_secret():
    return f"{SECRET_PREFIX}{secrets.token_urlsafe(SECRET_BYTES)}"


def hash_client_secret(secret: str):
    digest = hashlib.sha256(secret.encode("utf-8")).hexdigest()
    return f"{SECRET_HASH_PREFIX}{digest}"


def verify_client_secret(secret: str, secret_hash: str):
    if not secret_hash.startswith(SECRET_HASH_PREFIX):
        return False

    return hmac.compare_digest(hash_client_secret(secret), secret_hash)
