import asyncio
import json
import logging

import websockets
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
    ):
        self.config = config
        self.viewer_id = viewer_id
        self.channel_id = str(channel_id).strip()
        self.channel_name = str(self.config.get("kick_channel", "")).strip()
        self.channel = self.channel_name
        self._running = True
        self._viewer_token: str | None = None
        self._http_session = requests.Session()
        self._websocket = None
        self._session_cookies = {}
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

        try:
            viewer_token = await asyncio.to_thread(self._get_viewer_token_sync)
            if not viewer_token:
                raise RuntimeError(
                    f"Viewer {self.viewer_id}: failed to get viewer token via HTTP"
                )
            self._viewer_token = viewer_token
            self._session_cookies = self._http_session.cookies.get_dict()
            cookie_string = "; ".join(
                f"{key}={value}" for key, value in self._session_cookies.items()
            )
            logger.info(
                "viewer=%s successfully obtained viewer token via HTTP",
                self.viewer_id,
            )

            uri = "wss://websockets.kick.com/viewer/v1/connect"
            headers = {
                "Cookie": cookie_string,
                "User-Agent": self.user_agent,
                "Origin": "https://kick.com",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            }
            subscribe_payload = {
                "event": "pusher:subscribe",
                "data": {
                    "auth": viewer_token,
                    "channel": f"livestream.{self.channel_id}.viewers",
                },
            }

            async with websockets.connect(uri, extra_headers=headers) as websocket:
                self._websocket = websocket
                await websocket.send(json.dumps(subscribe_payload))
                logger.info(
                    "viewer=%s subscribed websocket channel_id=%s",
                    self.viewer_id,
                    self.channel_id,
                )

                while not self._stop_event.is_set():
                    try:
                        message = await websocket.recv()
                    except websockets.ConnectionClosed:
                        if not self._stop_event.is_set():
                            logger.warning(
                                "viewer=%s websocket connection closed",
                                self.viewer_id,
                                exc_info=True,
                            )
                        break

                    try:
                        payload = json.loads(message)
                    except json.JSONDecodeError:
                        continue

                    if (
                        isinstance(payload, dict)
                        and payload.get("event") == "pusher:ping"
                    ):
                        await websocket.send(
                            json.dumps({"event": "pusher:pong", "data": {}})
                        )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("Viewer %s failed: %s", self.viewer_id, e, exc_info=True)
            raise
        finally:
            self._running = False
            self._stop_event.set()
            self._websocket = None
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
                    "viewer=%s ignored HTTP session close error",
                    self.viewer_id,
                    exc_info=True,
                )

    async def stop(self) -> None:
        self._stop_event.set()
        websocket = self._websocket
        if websocket:
            try:
                await websocket.close()
            except Exception:
                logger.debug(
                    "viewer=%s ignored websocket close error",
                    self.viewer_id,
                    exc_info=True,
                )
        await asyncio.to_thread(self._http_session.close)
