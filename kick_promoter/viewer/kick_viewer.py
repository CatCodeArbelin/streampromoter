import asyncio
import contextlib
import logging
import random

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
    ):
        self.config = config
        self.viewer_id = viewer_id
        self.channel_name = str(self.config.get("kick_channel", "")).strip()
        self.channel = self.channel_name
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
        user_agents = self.config.get("user_agents")
        self.user_agent = (
            random.choice(user_agents)
            if isinstance(user_agents, list) and user_agents
            else DEFAULT_USER_AGENT
        )
        logger.info(
            "component=kick_viewer event=initialized viewer=%s channel_name=%s ua=%s",
            self.viewer_id,
            self.channel_name,
            self.user_agent,
        )

    async def run(self) -> None:
        if not self.channel:
            raise RuntimeError("kick_channel is required")

        try:
            if self._startup_jitter_max > 0:
                await asyncio.sleep(random.uniform(0, self._startup_jitter_max))

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

                async with self._page.expect_response(
                    lambda response: "/viewer/v1/token" in response.url
                    and response.status == 200,
                    timeout=self._playwright_response_timeout_ms,
                ) as response_info:
                    await self._page.goto(
                        f"https://kick.com/{self.channel}",
                        wait_until="domcontentloaded",
                    )

                response = await response_info.value
                data = await response.json()
                token = data.get("data", {}).get("token")
                if not token:
                    raise RuntimeError(
                        f"Viewer {self.viewer_id}: failed to get viewer token via browser"
                    )

                logger.info(
                    "viewer=%s successfully obtained viewer token via browser",
                    self.viewer_id,
                )
                await self._stop_event.wait()
        except Exception as e:
            logger.error("Viewer %s failed: %s", self.viewer_id, e, exc_info=True)
            raise
        finally:
            self._running = False
            self._stop_event.set()
            await self._close_browser_resources()

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
