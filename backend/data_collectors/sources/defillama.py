from __future__ import annotations

from typing import Any

import requests

DEFILLAMA_PROTOCOLS_URL = "https://api.llama.fi/protocols"
DEFILLAMA_YIELDS_URL = "https://yields.llama.fi/pools"


def _request_json(url: str, timeout_sec: int) -> tuple[bool, Any, str | None]:
    try:
        response = requests.get(url, timeout=timeout_sec)
        response.raise_for_status()
        return True, response.json(), None
    except requests.exceptions.HTTPError as exc:
        return False, None, f"HTTP error: {exc}"
    except requests.exceptions.RequestException as exc:
        return False, None, f"Network error: {exc}"
    except ValueError as exc:
        return False, None, f"Invalid JSON: {exc}"


def fetch_defillama_snapshot(timeout_sec: int = 30) -> dict[str, Any]:
    ok, payload, error = _request_json(DEFILLAMA_PROTOCOLS_URL, timeout_sec)
    if not ok:
        return {
            "source": "defillama_protocols",
            "ok": False,
            "error": error,
            "protocols_total": 0,
            "protocols": [],
        }

    if not isinstance(payload, list):
        return {
            "source": "defillama_protocols",
            "ok": False,
            "error": f"Unexpected payload type: {type(payload).__name__}",
            "protocols_total": 0,
            "protocols": [],
        }

    # ограничение размера, чтобы не брать весь массив
    protocols = payload[:100]
    return {
        "source": "defillama_protocols",
        "ok": True,
        "error": None,
        "protocols_total": len(payload),
        "protocols": protocols,
    }


def fetch_defillama_yields_snapshot(timeout_sec: int = 30) -> dict[str, Any]:
    ok, payload, error = _request_json(DEFILLAMA_YIELDS_URL, timeout_sec)
    if not ok:
        return {
            "source": "defillama_yields",
            "ok": False,
            "error": error,
            "pools_total": 0,
            "pools": [],
        }

    # У yields API формат {"status": "...", "data": [...]}.
    pools: list[dict[str, Any]] = []
    if isinstance(payload, dict) and isinstance(payload.get("data"), list):
        pools = [p for p in payload["data"] if isinstance(p, dict)]
    elif isinstance(payload, list):
        pools = [p for p in payload if isinstance(p, dict)]
    else:
        return {
            "source": "defillama_yields",
            "ok": False,
            "error": f"Unexpected payload type: {type(payload).__name__}",
            "pools_total": 0,
            "pools": [],
        }

    return {
        "source": "defillama_yields",
        "ok": True,
        "error": None,
        "pools_total": len(pools),
        "pools": pools[:100],
    }