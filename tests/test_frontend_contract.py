"""Frontend <-> API contract: every backend path the Next.js source calls must exist in the FastAPI OpenAPI schema,
and every router group is consumed by at least one page. Needs no running server or database.

Run:  pytest -q tests/test_frontend_contract.py
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from src.api.config import ApiSettings
from src.api.main import create_app

SRC = Path(__file__).resolve().parents[1] / "frontend" / "src"
LITERAL = re.compile(r"""(?P<q>[`'"])(?P<path>/(?:api/v1/[^`'"\s]*|health|ready))(?P=q)""")


def _client_paths() -> dict[str, set[str]]:
    """normalised path -> files that use it ("${...}" -> "{}" ; query strings removed)."""
    found: dict[str, set[str]] = {}
    for f in SRC.rglob("*.ts*"):
        if f.name.endswith(".d.ts"):
            continue
        for m in LITERAL.finditer(f.read_text(encoding="utf-8")):
            p = re.sub(r"\$\{[^}]*\}", "{}", m.group("path")).split("?")[0].split("${")[0]
            found.setdefault(p.rstrip("/") or "/", set()).add(f.name)
    return found


@pytest.fixture(scope="module")
def openapi_paths() -> set[str]:
    app = create_app(ApiSettings(_env_file=None, env="test", enable_model_inference=False))
    return set(app.openapi()["paths"])


def _matches(client_path: str, api_path: str) -> bool:
    rx = "^" + "".join("[^/]+" if part.startswith("{") else re.escape(part) for part in re.split(r"(\{[^}]+\})", api_path)) + "$"
    return re.match(rx, client_path.replace("{}", "x")) is not None


def test_frontend_calls_something(openapi_paths):
    assert len(_client_paths()) >= 25


def test_every_frontend_endpoint_exists_in_the_openapi_schema(openapi_paths):
    missing = {p: sorted(fs) for p, fs in _client_paths().items() if not any(_matches(p, a) for a in openapi_paths)}
    assert not missing, f"frontend calls endpoints the API does not define: {missing}"


def test_core_router_groups_are_consumed_by_the_dashboard(openapi_paths):
    used = " ".join(_client_paths())
    for group in ("/api/v1/analytics", "/api/v1/hotspots", "/api/v1/predictions", "/api/v1/models", "/api/v1/sensors", "/api/v1/settings",
                  "/api/v1/streaming", "/api/v1/pipeline", "/api/v1/performance", "/api/v1/status"):
        assert group in used, f"no page uses {group}"
