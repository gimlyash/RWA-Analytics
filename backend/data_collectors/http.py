"""
Общий HTTP-клиент с повторными попытками и таймаутами.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import httpx

from backend.data_collectors.config import CollectorSettings

logger = logging.getLogger(__name__)


class HttpFetcher:
    """Обёртка над httpx с простым exponential backoff."""

    def __init__(self, settings: CollectorSettings) -> None:
        self._settings = settings
        self._client = httpx.Client(
            timeout=httpx.Timeout(settings.http_timeout_sec),
            follow_redirects=True,
            headers={
                "User-Agent": "RWA-Analytics/1.0 (diploma project; data collector)",
                "Accept": "application/json",
            },
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> HttpFetcher:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def get_json(self, url: str, *, extra_headers: dict[str, str] | None = None) -> Any:
        """
        Выполняет GET и парсит JSON.

        Raises:
            httpx.HTTPError: после исчерпания попыток.
        """

        headers = dict(self._client.headers)
        if extra_headers:
            headers.update(extra_headers)

        last_exc: Exception | None = None
        for attempt in range(1, self._settings.http_max_retries + 1):
            try:
                response = self._client.get(url, headers=headers)
                response.raise_for_status()
                return response.json()
            except (httpx.HTTPError, ValueError) as e:
                last_exc = e
                logger.warning(
                    "GET %s failed (attempt %s/%s): %s",
                    url,
                    attempt,
                    self._settings.http_max_retries,
                    e,
                )
                if attempt < self._settings.http_max_retries:
                    time.sleep(self._settings.http_retry_backoff_sec * attempt)

        assert last_exc is not None
        raise last_exc
