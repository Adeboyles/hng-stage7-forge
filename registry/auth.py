import os
import sys
import sqlite3
import secrets
import datetime
import argparse
from pathlib import Path
from fastapi import Request, HTTPException
import bcrypt
import yaml


def load_config():
    """Load platform configuration from config.yaml."""
    config_path = os.environ.get("FORGE_CONFIG", "config.yaml")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def _configured_path(key: str) -> Path:
    """
    Resolve config paths consistently between host CLIs and containers.

    The shared config uses container-style `/data/...` paths. Inside Docker we
    use them as-is. On the host, when that path does not exist, map it to the
    repo-local `data/...` next to the config file so `forge-token` uses the
    same mounted SQLite file as the registry service.
    """
    config_path = Path(os.environ.get("FORGE_CONFIG", "config.yaml")).resolve()
    config = load_config()
    raw_path = str(config["registry"][key])
    path = Path(raw_path)
    running_from_container_config = (
        Path("/.dockerenv").exists() and config_path.as_posix().startswith("/app/")
    )
    normalized = raw_path.replace("\\", "/")

    if not running_from_container_config:
        if normalized == "/data" or normalized.startswith("/data/"):
            relative = normalized.removeprefix("/").replace("/", os.sep)
            return (config_path.parent / relative).resolve()

    return path


def get_db():
    """Get a SQLite connection, creating the tokens table if needed."""
    db_path = _configured_path("db_path")
    os.makedirs(os.path.dirname(str(db_path)) or ".", exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tokens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            token_hash TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
    conn.commit()
    return conn


def create_token(name):
    """
    Generate a new auth token.
    Returns the raw token (shown once, never stored).
    Stores only the bcrypt hash in the database.
    """
    raw_token = "fg_" + secrets.token_hex(32)

    salt = bcrypt.gensalt(rounds=12)
    token_hash = bcrypt.hashpw(raw_token.encode("utf-8"), salt).decode("utf-8")

    created_at = datetime.datetime.now(datetime.UTC).isoformat().replace("+00:00", "Z")

    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO tokens (name, token_hash, created_at) VALUES (?, ?, ?)",
            (name, token_hash, created_at),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        raise ValueError(f"Token with name '{name}' already exists")
    finally:
        conn.close()

    return raw_token


def verify_token(raw_token):
    """
    Check a raw token against all stored hashes.
    Returns the token name (identity) if valid, None if not.
    """
    conn = get_db()
    rows = conn.execute("SELECT name, token_hash FROM tokens").fetchall()
    conn.close()

    for row in rows:
        if bcrypt.checkpw(raw_token.encode("utf-8"), row["token_hash"].encode("utf-8")):
            return row["name"]

    return None


async def get_token_identity(request: Request) -> str:
    """
    FastAPI dependency that enforces Bearer token auth.
    Use in route functions like:
        @app.post("/artifacts/{name}/{version}")
        async def upload(name: str, version: str, identity: str = Depends(get_token_identity)):
    """
    auth_header = request.headers.get("Authorization", "")

    if not auth_header.startswith("Bearer "):
        raise HTTPException(
            status_code=401, detail="Missing or invalid Authorization header"
        )

    raw_token = auth_header[7:]

    identity = verify_token(raw_token)
    if identity is None:
        raise HTTPException(status_code=401, detail="Invalid token")

    return identity


# Keep require_auth as a simple wrapper for compatibility
require_auth = get_token_identity


def list_tokens():
    """List all token names and creation dates (never the hashes)."""
    conn = get_db()
    rows = conn.execute("SELECT name, created_at FROM tokens").fetchall()
    conn.close()
    return [{"name": row["name"], "created_at": row["created_at"]} for row in rows]


def revoke_token(name):
    """Delete a token by name."""
    conn = get_db()
    cursor = conn.execute("DELETE FROM tokens WHERE name = ?", (name,))
    conn.commit()
    deleted = cursor.rowcount > 0
    conn.close()
    return deleted


def main():
    """Host-side token administration CLI."""
    parser = argparse.ArgumentParser(
        prog="forge-token", description="Forge token administration"
    )
    subparsers = parser.add_subparsers(dest="command")

    create_parser = subparsers.add_parser("create", help="Create a token")
    create_parser.add_argument("name", help="Token identity/name")

    subparsers.add_parser("list", help="List tokens")

    revoke_parser = subparsers.add_parser("revoke", help="Revoke a token")
    revoke_parser.add_argument("name", help="Token identity/name")

    args = parser.parse_args()

    try:
        if args.command == "create":
            token = create_token(args.name)
            print(f"Created token for {args.name}")
            print(token)
            print("Save this! It will never be shown again.")
            return

        if args.command == "list":
            tokens = list_tokens()
            if not tokens:
                print("No tokens found.")
                return
            for token in tokens:
                print(f"{token['name']}\t{token['created_at']}")
            return

        if args.command == "revoke":
            if revoke_token(args.name):
                print(f"Revoked token '{args.name}'")
                return
            print(f"Token '{args.name}' not found")
            sys.exit(1)

        parser.print_help()
        sys.exit(1)
    except Exception as exc:
        print(f"Error: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
