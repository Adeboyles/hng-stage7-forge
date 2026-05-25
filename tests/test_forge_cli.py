from __future__ import annotations

from argparse import Namespace
import builtins

import httpx
import pytest

import cli.forge as forge_cli


def test_cmd_login_reports_json_error_without_traceback(monkeypatch, capsys):
    monkeypatch.setattr(builtins, "input", lambda _prompt: "fg_bad")
    monkeypatch.setattr(
        forge_cli.httpx,
        "get",
        lambda *args, **kwargs: httpx.Response(
            401,
            json={"detail": "Invalid token"},
            request=httpx.Request("GET", "http://localhost/auth/verify"),
        ),
    )

    with pytest.raises(SystemExit) as exc_info:
        forge_cli.cmd_login(Namespace(url="http://localhost"))

    output = capsys.readouterr().out
    assert exc_info.value.code == 1
    assert "Login failed: Invalid token" in output


def test_cmd_login_reports_non_json_error_without_dumping_html(monkeypatch, capsys):
    monkeypatch.setattr(builtins, "input", lambda _prompt: "fg_bad")
    monkeypatch.setattr(
        forge_cli.httpx,
        "get",
        lambda *args, **kwargs: httpx.Response(
            502,
            text="<html><body>Bad Gateway</body></html>",
            headers={"content-type": "text/html"},
            request=httpx.Request("GET", "http://localhost/auth/verify"),
        ),
    )

    with pytest.raises(SystemExit) as exc_info:
        forge_cli.cmd_login(Namespace(url="http://localhost"))

    output = capsys.readouterr().out
    assert exc_info.value.code == 1
    assert "Login failed: Invalid token (HTTP 502, non-JSON response from server)" in output
    assert "<html>" not in output
