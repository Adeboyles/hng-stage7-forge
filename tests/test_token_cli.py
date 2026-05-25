import importlib
from pathlib import Path

import pytest


def test_token_cli_create_list_and_revoke(monkeypatch, tmp_path, capsys):
    config_path = tmp_path / "config.yaml"
    db_path = tmp_path / "registry.db"
    config_path.write_text(
        f"""
registry:
  db_path: {db_path.as_posix()}
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("FORGE_CONFIG", str(config_path))

    auth = importlib.import_module("registry.auth")
    importlib.reload(auth)

    monkeypatch.setattr("sys.argv", ["forge-token", "create", "admin"])
    auth.main()
    create_out = capsys.readouterr().out
    assert "fg_" in create_out
    assert "admin" in create_out

    monkeypatch.setattr("sys.argv", ["forge-token", "list"])
    auth.main()
    list_out = capsys.readouterr().out
    assert "admin" in list_out
    assert "fg_" not in list_out

    monkeypatch.setattr("sys.argv", ["forge-token", "revoke", "admin"])
    auth.main()
    revoke_out = capsys.readouterr().out
    assert "revoked" in revoke_out.lower()

    monkeypatch.setattr("sys.argv", ["forge-token", "list"])
    auth.main()
    final_list_out = capsys.readouterr().out
    assert "admin" not in final_list_out


def test_token_cli_create_duplicate_name_exits_cleanly(monkeypatch, tmp_path, capsys):
    config_path = tmp_path / "config.yaml"
    db_path = tmp_path / "registry.db"
    config_path.write_text(
        f"""
registry:
  db_path: {db_path.as_posix()}
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("FORGE_CONFIG", str(config_path))

    auth = importlib.import_module("registry.auth")
    importlib.reload(auth)

    monkeypatch.setattr("sys.argv", ["forge-token", "create", "admin"])
    auth.main()
    capsys.readouterr()

    monkeypatch.setattr("sys.argv", ["forge-token", "create", "admin"])
    with pytest.raises(SystemExit) as exc_info:
        auth.main()

    output = capsys.readouterr().out
    assert exc_info.value.code == 1
    assert "Error: Token with name 'admin' already exists" in output
    assert "Traceback" not in output


def test_token_cli_maps_container_data_db_path_to_repo_data_on_host(monkeypatch, tmp_path, capsys):
    repo_data = tmp_path / "data"
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
registry:
  db_path: /data/registry.db
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("FORGE_CONFIG", str(config_path))

    auth = importlib.import_module("registry.auth")
    importlib.reload(auth)

    monkeypatch.setattr("sys.argv", ["forge-token", "create", "admin"])
    auth.main()
    create_out = capsys.readouterr().out

    db_path = repo_data / "registry.db"
    assert "Created token for admin" in create_out
    assert db_path.exists()

    monkeypatch.setattr("sys.argv", ["forge-token", "list"])
    auth.main()
    list_out = capsys.readouterr().out
    assert "admin" in list_out
