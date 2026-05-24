import importlib
from pathlib import Path


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
