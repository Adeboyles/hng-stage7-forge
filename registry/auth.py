import os
import sqlite3
import secrets
import datetime
from functools import wraps
from fastapi import Request, HTTPException
import bcrypt
import yaml


def load_config():
    """Load platform configuration from config.yaml."""
    config_path = os.environ.get("FORGE_CONFIG", "config.yaml")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_db():
    """Get a SQLite connection, creating the tokens table if needed."""
    config = load_config()
    db_path = config["registry"]["db_path"]
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(db_path)
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

    created_at = datetime.datetime.utcnow().isoformat() + "Z"

    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO tokens (name, token_hash, created_at) VALUES (?, ?, ?)",
            (name, token_hash, created_at)
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
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")

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
