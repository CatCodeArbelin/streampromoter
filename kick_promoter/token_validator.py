import json
import logging
import re
from urllib.parse import urljoin

from kick_promoter.kick_http_client import KickHttpClient

logger = logging.getLogger(__name__)

# Match all script src attributes to collect JavaScript bundle URLs from HTML.
_SCRIPT_SRC_RE = re.compile(
    r"<script[^>]+src=[\"'](?P<src>[^\"']+)[\"'][^>]*>",
    re.IGNORECASE,
)

# Match a strict SHA-256-like token candidate: exactly 64 hex symbols.
_SHA256_HEX_RE = re.compile(r"\b(?P<token>[A-Fa-f0-9]{64})\b")


class TokenExpiredError(RuntimeError):
    """Raised when Kick static x-client-token differs from configured one."""


async def validate_x_client_token(config: dict, kick_http_client: KickHttpClient) -> bool:
    """Validate configured static Kick x-client-token against token candidate in Kick JS bundles.

    Steps:
    1) Read expected token from config['x_client_token'].
    2) Download https://kick.com homepage.
    3) Extract JS <script src="..."> links.
    4) Download up to first 5 JS files.
    5) Search each JS body for a 64-char hex candidate token.
    6) Raise TokenExpiredError if discovered token mismatches configured token.
    7) Log success and return True when token matches.

    Args:
        config: Runtime configuration dictionary with expected token in `x_client_token`.
        kick_http_client: Kick HTTP client used for Kick-friendly GET requests.

    Returns:
        bool: True if validation succeeds.

    Raises:
        RuntimeError: When `x_client_token` is missing in config.
        Exception: If kick.com homepage request fails.
        TokenExpiredError: If discovered static token does not match configured one.
    """
    expected_token: str = str(config.get("x_client_token", "")).strip()
    if not expected_token:
        raise RuntimeError("x_client_token is required")

    base_url: str = "https://kick.com"

    # 1) Load homepage HTML.
    html: str = await kick_http_client.get_text(base_url)

    # 2) Extract and normalize JS script URLs.
    script_urls: list[str] = []
    seen_urls: set[str] = set()

    for match in _SCRIPT_SRC_RE.finditer(html):
        raw_src: str = match.group("src").strip()
        if not raw_src:
            continue

        js_url: str = urljoin(base_url, raw_src)
        if js_url in seen_urls:
            continue

        seen_urls.add(js_url)
        if js_url.endswith(".js") or ".js?" in js_url:
            script_urls.append(js_url)

    # Only first 5 scripts to avoid unnecessary load.
    candidates: list[str] = script_urls[:5]

    discovered_token: str | None = None

    # 3) Scan candidate scripts for first 64-hex token.
    for js_url in candidates:
        try:
            js_body: str = await kick_http_client.get_text(js_url)
        except Exception as exc:
            logger.debug(
                "component=token_validator event=skip_script_error payload=%s",
                json.dumps({"url": js_url, "error": str(exc)}),
            )
            continue

        token_match = _SHA256_HEX_RE.search(js_body)
        if token_match:
            discovered_token = token_match.group("token")
            break

    # If no candidate token is found, treat it as a hard validation failure.
    if not discovered_token:
        raise RuntimeError("Could not find a 64-char static token candidate in Kick JS bundles")

    # 4) Compare discovered token with configured token (case-insensitive for hex).
    if discovered_token.lower() != expected_token.lower():
        raise TokenExpiredError(
            "Static token has been rotated by Kick. Please update x_client_token in config. "
            f"Found: {discovered_token}"
        )

    logger.info(
        "component=token_validator event=validation_passed payload=%s",
        json.dumps({"scripts_found": len(script_urls), "scripts_checked": len(candidates)}),
    )
    return True
