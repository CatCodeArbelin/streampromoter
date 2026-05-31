import asyncio
import logging
from typing import Any

from curl_cffi import requests

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
        browser_context: Any,
    ):
        self.config = config
        self.viewer_id = viewer_id
        self.channel_id = str(channel_id).strip()
        self.browser_context = browser_context
        self.channel_name = str(self.config.get("kick_channel", "")).strip()
        self.channel = self.channel_name
        self._running = True
        self._viewer_token: str | None = None
        self._http_session = requests.Session()
        self._stop_event = asyncio.Event()
        user_agents = self.config.get("user_agents")
        self.user_agent = (
            str(user_agents[0])
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

        page = None
        try:
            viewer_token = await self.get_viewer_token()
            session_token = str(self.config.get("session_token", "")).strip()
            if not session_token:
                raise RuntimeError("session_token is required to open Kick viewer page")

            page = await self.browser_context.new_page()
            await page.context.add_cookies(
                [
                    {
                        "name": "session_token",
                        "value": session_token,
                        "domain": ".kick.com",
                        "path": "/",
                        "httpOnly": True,
                        "secure": True,
                        "sameSite": "Lax",
                    }
                ]
            )

            viewer_url = f"https://kick.com/{self.channel}"
            logger.info(
                "viewer=%s opening Kick page channel=%s channel_id=%s",
                self.viewer_id,
                self.channel,
                self.channel_id,
            )
            await page.goto(viewer_url, wait_until="domcontentloaded")
            await page.evaluate(
                """
                ({ viewerToken, channelId }) => {
                    const previousSocket = window.__kickViewerSocket;
                    if (previousSocket && previousSocket.readyState < WebSocket.CLOSING) {
                        previousSocket.close();
                    }

                    const socket = new WebSocket("wss://websockets.kick.com/viewer/v1/connect");
                    window.__kickViewerSocket = socket;
                    socket.onopen = () => {
                        socket.send(JSON.stringify({
                            event: "pusher:subscribe",
                            data: {
                                auth: viewerToken,
                                channel: `livestream.${channelId}.viewers`,
                            },
                        }));
                    };
                    socket.onmessage = (event) => {
                        try {
                            const payload = JSON.parse(event.data);
                            if (payload && payload.event === "pusher:ping") {
                                socket.send(JSON.stringify({ event: "pusher:pong", data: {} }));
                            }
                        } catch (error) {
                            // Ignore non-JSON websocket messages.
                        }
                    };
                }
                """,
                {"viewerToken": viewer_token, "channelId": self.channel_id},
            )
            logger.info(
                "viewer=%s subscribed browser websocket channel_id=%s",
                self.viewer_id,
                self.channel_id,
            )
            await self._stop_event.wait()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("Viewer %s failed: %s", self.viewer_id, e, exc_info=True)
            raise
        finally:
            self._running = False
            self._stop_event.set()
            if page:
                try:
                    await page.close()
                except Exception:
                    logger.debug(
                        "viewer=%s ignored page close error",
                        self.viewer_id,
                        exc_info=True,
                    )
            await self._close_http_session()

    async def get_viewer_token(self) -> str:
        token = await asyncio.to_thread(self._get_viewer_token_sync)
        if not token:
            raise RuntimeError(
                f"Viewer {self.viewer_id}: failed to get viewer token via HTTP"
            )
        self._viewer_token = token
        logger.info(
            "viewer=%s successfully obtained viewer token via HTTP", self.viewer_id
        )
        return token

    def _get_viewer_token_sync(self) -> str:
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
        response = self._http_session.get(
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

    async def _close_http_session(self) -> None:
        session = self._http_session
        if session:
            try:
                await asyncio.to_thread(session.close)
            except Exception:
                logger.debug(
                    "viewer=%s ignored HTTP session close error", self.viewer_id,
                    exc_info=True,
                )

    async def stop(self) -> None:
        self._stop_event.set()
        await self._close_http_session()
