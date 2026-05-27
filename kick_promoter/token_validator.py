import logging
import re
from urllib.parse import urljoin

import aiohttp

logger = logging.getLogger(__name__)

_SCRIPT_SRC_RE = re.compile(r"<script[^>]+src=[\"'](?P<src>[^\"']+)[\"'][^>]*>", re.IGNORECASE)
_TOKEN_RE = re.compile(
    r"(?:x[-_]?client[-_]?token|x_client_token|x-client-token)[^A-Fa-f0-9]{0,80}(?P<token>[A-Fa-f0-9]{24,128})",
    re.IGNORECASE,
)


class TokenExpiredError(RuntimeError):
    pass


async def validate_x_client_token(config: dict, session: aiohttp.ClientSession) -> None:
    configured_token = str(config.get("x_client_token", "")).strip()
    if not configured_token:
        raise RuntimeError("x_client_token is required")

    base_url = "https://kick.com/"
    async with session.get(base_url) as response:
        response.raise_for_status()
        html = await response.text()

    script_urls = []
    seen = set()
    for match in _SCRIPT_SRC_RE.finditer(html):
        src = match.group("src").strip()
        if not src:
            continue
        full_url = urljoin(base_url, src)
        if full_url in seen:
            continue
        seen.add(full_url)
        if full_url.endswith(".js") or ".js?" in full_url:
            script_urls.append(full_url)

    relevant_urls = [
        url
        for url in script_urls
        if any(marker in url.lower() for marker in ("app", "main", "index", "bundle", "chunk", "webpack"))
    ]
    candidates = relevant_urls or script_urls
    candidates = candidates[:12]

    discovered_token = None
    for bundle_url in candidates:
        try:
            async with session.get(bundle_url) as bundle_response:
                if bundle_response.status != 200:
                    continue
                bundle_text = await bundle_response.text()
        except aiohttp.ClientError:
            continue

        match = _TOKEN_RE.search(bundle_text)
        if match:
            discovered_token = match.group("token")
            break

    if not discovered_token:
        logger.warning(
            "component=token_validator event=token_not_found_in_bundles script_count=%s candidate_count=%s",
            len(script_urls),
            len(candidates),
        )
        return

    if discovered_token.lower() != configured_token.lower():
        logger.error(
            "component=token_validator event=validation_failed configured_prefix=%s discovered_prefix=%s",
            configured_token[:8],
            discovered_token[:8],
        )
        raise TokenExpiredError(
            "Static token has been rotated by Kick. Please update x_client_token in config."
        )

    logger.info(
        "component=token_validator event=validation_passed script_count=%s candidate_count=%s",
        len(script_urls),
        len(candidates),
    )
