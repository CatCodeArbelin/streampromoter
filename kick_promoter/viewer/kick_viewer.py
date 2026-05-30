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
        self._websocket = None
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

        try:
            await self.get_viewer_token()
            cookie_str = self._get_cookie_header()
            if not cookie_str:
                logger.warning(
                    "viewer=%s no HTTP cookies were captured before websocket connect",
                    self.viewer_id,
                )

            headers = {
                "Cookie": cookie_str,
                "User-Agent": self.user_agent,
                "Origin": "https://kick.com",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            }
            uri = "wss://websockets.kick.com/viewer/v1/connect"

            logger.info(
                "viewer=%s connecting websocket channel_id=%s cookies=%s",
                self.viewer_id,
                self.channel_id,
                bool(cookie_str),
            )
            async with websockets.connect(
                uri,
                extra_headers=headers,
                user_agent_header=None,
                ping_interval=20,
                ping_timeout=20,
                open_timeout=10,
            ) as websocket:
                self._websocket = websocket
                await self._subscribe_to_viewers_channel(websocket)
                await self._listen_until_stopped(websocket)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("Viewer %s failed: %s", self.viewer_id, e, exc_info=True)
            raise
        finally:
            self._running = False
            self._stop_event.set()
            await self._close_websocket()
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

    def _get_cookie_header(self) -> str:
        cookies = self._http_session.cookies.get_dict()
        return "; ".join(f"{key}={value}" for key, value in cookies.items())

    async def _subscribe_to_viewers_channel(self, websocket) -> None:
        if not self._viewer_token:
            raise RuntimeError("viewer token is not initialized")

        payload = {
            "event": "pusher:subscribe",
            "data": {
                "auth": self._viewer_token,
                "channel": f"livestream.{self.channel_id}.viewers",
            },
        }
        await websocket.send(json.dumps(payload))
        logger.info(
            "viewer=%s subscribed viewer websocket channel_id=%s",
            self.viewer_id,
            self.channel_id,
        )

    async def _listen_until_stopped(self, websocket) -> None:
        while not self._stop_event.is_set():
            receive_task = asyncio.create_task(websocket.recv())
            stop_task = asyncio.create_task(self._stop_event.wait())
            done, pending = await asyncio.wait(
                {receive_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            if stop_task in done:
                break

            message = receive_task.result()
            await self._handle_websocket_message(websocket, message)

    async def _handle_websocket_message(self, websocket, message: str | bytes) -> None:
        if isinstance(message, bytes):
            message = message.decode("utf-8", errors="ignore")
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            logger.debug(
                "viewer=%s received non-json websocket message", self.viewer_id
            )
            return

        if payload.get("event") == "pusher:ping":
            await websocket.send(json.dumps({"event": "pusher:pong", "data": {}}))
            logger.debug("viewer=%s sent pusher pong", self.viewer_id)

    async def _close_websocket(self) -> None:
        websocket = self._websocket
        self._websocket = None
        if websocket:
            try:
                await websocket.close()
            except Exception:
                logger.debug(
                    "viewer=%s ignored websocket close error", self.viewer_id,
                    exc_info=True,
                )

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
        await self._close_websocket()
        await self._close_http_session()
