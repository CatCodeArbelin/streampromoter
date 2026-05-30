import asyncio
import contextlib
import logging
import random
from collections.abc import Callable

from playwright.async_api import async_playwright

from kick_promoter.viewer.token_limiter import TokenRateLimiter

logger = logging.getLogger(__name__)
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
)


class KickViewer:
    def __init__(
        self,
        config: dict,
        viewer_id: int,
        channel_id: str,
        chatroom_id: str,
        token_limiter: TokenRateLimiter,
        on_ws_connection_change: Callable[[int], None] | None = None,
    ):
        self.config = config
        self.viewer_id = viewer_id
        self.channel_id = channel_id
        self.chatroom_id = chatroom_id
        self.channel_name = str(self.config.get("kick_channel", "")).strip()
        self._token_limiter = token_limiter
        self._running = True
        self._browser = None
        self._context = None
        self._page = None
        self._stop_event = asyncio.Event()
        self._startup_jitter_max = float(self.config.get("startup_jitter_max", 2))
        self._playwright_navigation_timeout_ms = int(
            self.config.get("playwright_navigation_timeout_ms", 30000)
        )
        self._playwright_response_timeout_ms = int(
            self.config.get("playwright_response_timeout_ms", 15000)
        )
        self._on_ws_connection_change = on_ws_connection_change
        self._browser_viewer_active = False
        user_agents = self.config.get("user_agents")
        self.user_agent = (
            random.choice(user_agents)
            if isinstance(user_agents, list) and user_agents
            else DEFAULT_USER_AGENT
        )
        logger.info(
            "component=kick_viewer event=initialized viewer=%s channel_id=%s chatroom_id=%s channel_name=%s ua=%s",
            self.viewer_id,
            self.channel_id,
            self.chatroom_id,
            self.channel_name,
            self.user_agent,
        )

    async def run(self) -> None:
        channel_name = self.channel_name
        if not channel_name:
            raise RuntimeError("kick_channel is required")

        session_token = str(self.config.get("session_token", "")).strip()
        if not session_token:
            raise RuntimeError("session_token is required to run viewer browser")

        try:
            if self._startup_jitter_max > 0:
                await asyncio.sleep(random.uniform(0, self._startup_jitter_max))

            logger.info(
                "viewer=%s launching browser for channel=%s",
                self.viewer_id,
                channel_name,
            )
            async with async_playwright() as p:
                self._browser = await p.chromium.launch(headless=True)
                self._context = await self._browser.new_context(
                    user_agent=self.user_agent
                )
                await self._context.add_cookies(
                    [
                        {
                            "name": "session_token",
                            "value": session_token,
                            "domain": ".kick.com",
                            "path": "/",
                            "secure": True,
                            "httpOnly": True,
                            "sameSite": "Lax",
                        }
                    ]
                )
                self._page = await self._context.new_page()
                self._page.set_default_timeout(self._playwright_response_timeout_ms)
                self._page.set_default_navigation_timeout(
                    self._playwright_navigation_timeout_ms
                )
                await self._page.goto(
                    f"https://kick.com/{channel_name}",
                    wait_until="networkidle",
                    timeout=self._playwright_navigation_timeout_ms,
                )

                logger.info("viewer=%s opened Kick page", self.viewer_id)
                self._mark_browser_viewer_active()
                await self._stop_event.wait()
        finally:
            self._running = False
            self._stop_event.set()
            await self._close_browser_resources()

    def _mark_browser_viewer_active(self) -> None:
        if self._browser_viewer_active:
            return
        self._browser_viewer_active = True
        if self._on_ws_connection_change:
            self._on_ws_connection_change(1)

    def _mark_browser_viewer_inactive(self) -> None:
        if not self._browser_viewer_active:
            return
        self._browser_viewer_active = False
        if self._on_ws_connection_change:
            self._on_ws_connection_change(-1)

    async def _close_browser_resources(self) -> None:
        self._mark_browser_viewer_inactive()
        page = self._page
        context = self._context
        browser = self._browser
        self._page = None
        self._context = None
        self._browser = None

        for resource in (page, context, browser):
            if resource:
                with contextlib.suppress(Exception):
                    await resource.close()

    async def stop(self) -> None:
        self._running = False
        self._stop_event.set()
        await self._close_browser_resources()
