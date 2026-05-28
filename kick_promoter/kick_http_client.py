import asyncio
import logging
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import cloudscraper
from curl_cffi import requests as curl_requests

logger = logging.getLogger(__name__)


class KickHttpClient:
    """HTTP client with Kick-friendly anti-bot transports.

    The primary transport uses ai-cloudscraper's synchronous hybrid interpreter inside
    a worker thread. If that transport cannot return a successful response, requests
    are retried through curl_cffi with a Chrome impersonation fingerprint.
    """

    def __init__(self, config: dict):
        self.config = config
        self.google_api_key = config.get("google_api_key")

    async def get_json(self, url: str, **kwargs: Any) -> dict:
        """Fetch a URL and return the parsed JSON response body."""
        try:
            result = await asyncio.to_thread(self._cloudscraper_get_json, url, **kwargs)
            logger.info(
                "component=kick_http_client event=transport_succeeded transport=ai_cloudscraper url=%s",
                self._safe_url_for_log(url),
            )
            return result
        except Exception as exc:
            logger.warning(
                "component=kick_http_client event=transport_failed transport=ai_cloudscraper url=%s error_type=%s",
                self._safe_url_for_log(url),
                type(exc).__name__,
            )

        result = await asyncio.to_thread(self._curl_cffi_get_json, url, **kwargs)
        logger.info(
            "component=kick_http_client event=transport_succeeded transport=curl_cffi url=%s",
            self._safe_url_for_log(url),
        )
        return result

    async def get_text(self, url: str, **kwargs: Any) -> str:
        """Fetch a URL and return the response body as text."""
        try:
            result = await asyncio.to_thread(self._cloudscraper_get_text, url, **kwargs)
            logger.info(
                "component=kick_http_client event=transport_succeeded transport=ai_cloudscraper url=%s",
                self._safe_url_for_log(url),
            )
            return result
        except Exception as exc:
            logger.warning(
                "component=kick_http_client event=transport_failed transport=ai_cloudscraper url=%s error_type=%s",
                self._safe_url_for_log(url),
                type(exc).__name__,
            )

        result = await asyncio.to_thread(self._curl_cffi_get_text, url, **kwargs)
        logger.info(
            "component=kick_http_client event=transport_succeeded transport=curl_cffi url=%s",
            self._safe_url_for_log(url),
        )
        return result

    def _cloudscraper_get_json(self, url: str, **kwargs: Any) -> dict:
        response = self._cloudscraper_get(url, **kwargs)
        return response.json()

    def _cloudscraper_get_text(self, url: str, **kwargs: Any) -> str:
        response = self._cloudscraper_get(url, **kwargs)
        return response.text

    def _cloudscraper_get(self, url: str, **kwargs: Any):
        scraper = cloudscraper.create_scraper(
            interpreter="hybrid",
            google_api_key=self.google_api_key,
        )
        response = scraper.get(url, **kwargs)
        response.raise_for_status()
        return response

    def _curl_cffi_get_json(self, url: str, **kwargs: Any) -> dict:
        response = self._curl_cffi_get(url, **kwargs)
        return response.json()

    def _curl_cffi_get_text(self, url: str, **kwargs: Any) -> str:
        response = self._curl_cffi_get(url, **kwargs)
        return response.text

    def _curl_cffi_get(self, url: str, **kwargs: Any):
        session = curl_requests.Session()
        try:
            response = session.get(url, impersonate="chrome124", **kwargs)
            response.raise_for_status()
            return response
        finally:
            session.close()

    @staticmethod
    def _safe_url_for_log(url: str) -> str:
        """Return a URL without query/fragment values so secrets are not logged."""
        parsed_url = urlsplit(url)
        return urlunsplit((parsed_url.scheme, parsed_url.netloc, parsed_url.path, "", ""))
