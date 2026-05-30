import asyncio
import contextlib
import json
import logging
import random
from collections.abc import Callable

from playwright.async_api import async_playwright
import websockets

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
        self._playwright = None
        self._stop_event = asyncio.Event()
        self._startup_jitter_max = float(self.config.get("startup_jitter_max", 2))
        self._reconnect_base_delay = float(self.config.get("reconnect_base_delay", 1))
        self._max_reconnect_attempts = int(self.config.get("max_reconnect_attempts", 0))
        self._playwright_navigation_timeout_ms = int(
            self.config.get("playwright_navigation_timeout_ms", 30000)
        )
        self._playwright_response_timeout_ms = int(
            self.config.get("playwright_response_timeout_ms", 15000)
        )
        self._reconnect_max_delay = 30.0
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
        chat_task = None
        if self.config.get("chat_token", ""):
            chat_task = asyncio.create_task(self._chat_reconnect_loop(self.chatroom_id))

        try:
            reconnect_attempt = 0
            reconnect_delay = self._reconnect_base_delay
            if self._startup_jitter_max > 0:
                await asyncio.sleep(random.uniform(0, self._startup_jitter_max))
            while self._running:
                try:
                    await self._run_with_browser()
                    reconnect_attempt = 0
                    reconnect_delay = self._reconnect_base_delay
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    await self._close_browser_resources()
                    if not self._running:
                        break
                    reconnect_attempt += 1
                    if (
                        self._max_reconnect_attempts
                        and reconnect_attempt > self._max_reconnect_attempts
                    ):
                        logger.error(
                            "viewer=%s browser reconnect attempts exceeded max=%s",
                            self.viewer_id,
                            self._max_reconnect_attempts,
                        )
                        break
                    logger.warning(
                        "viewer=%s browser reconnect attempt=%s delay=%.1fs error=%s",
                        self.viewer_id,
                        reconnect_attempt,
                        reconnect_delay,
                        exc,
                    )
                    await asyncio.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, self._reconnect_max_delay)
        finally:
            self._running = False
            self._stop_event.set()
            if chat_task:
                chat_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await chat_task
            await self._close_browser_resources()

    async def _run_with_browser(self) -> None:
        channel_name = self.channel_name
        if not channel_name:
            raise RuntimeError("kick_channel is required")

        session_token = str(self.config.get("session_token", "")).strip()
        if not session_token:
            raise RuntimeError("session_token is required to run viewer browser")

        logger.info(
            "viewer=%s launching browser for channel=%s",
            self.viewer_id,
            channel_name,
        )
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(headless=True)
        self._context = await self._browser.new_context(user_agent=self.user_agent)
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

        async with self._page.expect_response(
            lambda response: "/viewer/v1/token" in response.url,
            timeout=self._playwright_response_timeout_ms,
        ) as response_info:
            await self._page.goto(
                f"https://kick.com/{channel_name}",
                wait_until="networkidle",
                timeout=self._playwright_navigation_timeout_ms,
            )
        response = await response_info.value
        payload = await response.json()
        token_data = payload.get("data", {}) if isinstance(payload, dict) else {}
        viewer_token = token_data.get("token") if isinstance(token_data, dict) else None
        if not viewer_token:
            raise RuntimeError("Failed to get viewer token via browser")

        logger.info("viewer=%s obtained viewer token via browser", self.viewer_id)
        self._mark_browser_viewer_active()
        await self._stop_event.wait()

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
        playwright = self._playwright
        self._page = None
        self._context = None
        self._browser = None
        self._playwright = None

        for resource in (page, context, browser):
            if resource:
                with contextlib.suppress(Exception):
                    await resource.close()
        if playwright:
            with contextlib.suppress(Exception):
                await playwright.stop()

    async def _connect_chat_loop(self, chatroom_id: str) -> None:
        uri = "wss://ws-us2.pusher.com/app/32cbd69e4b950bf97679"
        extra_headers = {
            "User-Agent": self.user_agent,
            "Origin": "https://kick.com",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Sec-WebSocket-Version": "13",
        }
        async with websockets.connect(
            uri,
            ping_interval=None,
            extra_headers=extra_headers,
        ) as ws:
            await ws.send(
                json.dumps(
                    {
                        "event": "pusher:subscribe",
                        "data": {"channel": f"chatrooms.{chatroom_id}.v2"},
                    }
                )
            )
            async for message in ws:
                logger.debug("viewer=%s chat-msg=%s", self.viewer_id, message)

    async def _chat_reconnect_loop(self, chatroom_id: str) -> None:
        while self._running:
            try:
                await self._connect_chat_loop(chatroom_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "viewer=%s chat ws reconnect after error: %s",
                    self.viewer_id,
                    exc,
                )
                await asyncio.sleep(3)

    async def stop(self) -> None:
        self._running = False
        self._stop_event.set()
        await self._close_browser_resources()
