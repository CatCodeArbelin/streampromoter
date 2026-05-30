import asyncio
import contextlib
import logging
import random

from curl_cffi import requests as curl_requests
from playwright.async_api import async_playwright

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
    ):
        self.config = config
        self.viewer_id = viewer_id
        self.channel_id = str(channel_id).strip()
        self.channel_name = str(self.config.get("kick_channel", "")).strip()
        self.channel = self.channel_name
        self._running = True
        self._browser = None
        self._context = None
        self._page = None
        self._viewer_token: str | None = None
        self._stop_event = asyncio.Event()
        self._startup_jitter_max = float(self.config.get("startup_jitter_max", 2))
        self._playwright_navigation_timeout_ms = int(
            self.config.get("playwright_navigation_timeout_ms", 30000)
        )
        user_agents = self.config.get("user_agents")
        self.user_agent = (
            random.choice(user_agents)
            if isinstance(user_agents, list) and user_agents
            else DEFAULT_USER_AGENT
        )
        logger.info(
            "component=kick_viewer event=initialized viewer=%s channel_id=%s channel_name=%s ua=%s",
            self.viewer_id,
            self.channel_id,
            self.channel_name,
            self.user_agent,
        )

    async def run(self) -> None:
        if not self.channel:
            raise RuntimeError("kick_channel is required")
        if not self.channel_id:
            raise RuntimeError("channel_id is required")

        try:
            if self._startup_jitter_max > 0:
                await asyncio.sleep(random.uniform(0, self._startup_jitter_max))

            self._viewer_token = await self._get_token_via_http()

            logger.info(
                "viewer=%s launching browser for channel=%s",
                self.viewer_id,
                self.channel,
            )
            async with async_playwright() as p:
                self._browser = await p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox"],
                )
                self._context = await self._browser.new_context(
                    user_agent=self.user_agent,
                    viewport={"width": 1920, "height": 1080},
                )

                session_token = self.config.get("session_token")
                if session_token:
                    await self._context.add_cookies(
                        [
                            {
                                "name": "session_token",
                                "value": str(session_token),
                                "domain": ".kick.com",
                                "path": "/",
                                "secure": True,
                                "httpOnly": True,
                                "sameSite": "Lax",
                            }
                        ]
                    )

                self._page = await self._context.new_page()
                self._page.set_default_navigation_timeout(
                    self._playwright_navigation_timeout_ms
                )
                await self._page.goto(
                    f"https://kick.com/{self.channel}",
                    wait_until="domcontentloaded",
                )
                await self._open_viewer_websocket()

                await self._stop_event.wait()
        except Exception as e:
            logger.error("Viewer %s failed: %s", self.viewer_id, e, exc_info=True)
            raise
        finally:
            self._running = False
            self._stop_event.set()
            await self._close_browser_resources()

    async def _get_token_via_http(self) -> str:
        token = await asyncio.to_thread(self._get_token_via_http_sync)
        if not token:
            raise RuntimeError(
                f"Viewer {self.viewer_id}: failed to get viewer token via HTTP"
            )
        logger.info(
            "viewer=%s successfully obtained viewer token via HTTP", self.viewer_id
        )
        return token

    def _get_token_via_http_sync(self) -> str:
        session_token = str(self.config.get("session_token", "")).strip()
        if not session_token:
            raise RuntimeError("session_token is required to get viewer token")

        client_token = str(
            self.config.get("viewer_token") or self.config.get("x_client_token") or ""
        ).strip()
        if not client_token:
            raise RuntimeError(
                "x_client_token or viewer_token is required to get viewer token"
            )

        headers = {
            "User-Agent": self.user_agent,
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Authorization": f"Bearer {session_token}",
            "Origin": "https://kick.com",
            "Referer": f"https://kick.com/{self.channel}",
            "x-app-platform": "web",
            "x-client-token": client_token,
        }
        session = curl_requests.Session()
        try:
            response = session.get(
                "https://websockets.kick.com/viewer/v1/token",
                headers=headers,
                impersonate="chrome124",
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict):
                return ""
            token = data.get("data", {}).get("token")
            return str(token).strip() if token else ""
        finally:
            session.close()

    async def _open_viewer_websocket(self) -> None:
        if not self._page:
            raise RuntimeError("viewer page is not initialized")
        if not self._viewer_token:
            raise RuntimeError("viewer token is not initialized")

        await self._page.evaluate(
            """
            ({ token, channelId }) => {
                if (window.__kickViewerSocket) {
                    window.__kickViewerSocket.close();
                }

                const ws = new WebSocket("wss://websockets.kick.com/viewer/v1/connect");
                window.__kickViewerSocket = ws;
                ws.onopen = () => ws.send(JSON.stringify({
                    event: "pusher:subscribe",
                    data: {
                        auth: token,
                        channel: `livestream.${channelId}.viewers`,
                    },
                }));
                ws.onmessage = (message) => {
                    try {
                        const payload = JSON.parse(message.data);
                        if (payload.event === "pusher:ping") {
                            ws.send(JSON.stringify({ event: "pusher:pong", data: {} }));
                        }
                    } catch (error) {
                        // Ignore non-JSON websocket frames.
                    }
                };
            }
            """,
            {"token": self._viewer_token, "channelId": self.channel_id},
        )
        logger.info(
            "viewer=%s opened browser viewer websocket for channel_id=%s",
            self.viewer_id,
            self.channel_id,
        )

    async def _close_browser_resources(self) -> None:
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
        self._stop_event.set()
