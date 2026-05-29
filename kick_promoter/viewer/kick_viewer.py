import asyncio
import json
import logging
import contextlib
import random
from collections.abc import Callable
from typing import Optional

import aiohttp
from curl_cffi import requests as curl_requests
import websockets

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
        on_ws_connection_change: Callable[[int], None] | None = None,
    ):
        self.config = config
        self.viewer_id = viewer_id
        self.channel_id = channel_id
        self.chatroom_id = chatroom_id
        self._running = True
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._ws_uri = "wss://websockets.kick.com/viewer/v1/connect"
        self._startup_jitter_max = float(self.config.get("startup_jitter_max", 2))
        self._reconnect_base_delay = float(self.config.get("reconnect_base_delay", 1))
        self._max_reconnect_attempts = int(self.config.get("max_reconnect_attempts", 0))
        self._reconnect_max_delay = 30.0
        self._on_ws_connection_change = on_ws_connection_change
        user_agents = self.config.get("user_agents")
        self.user_agent = random.choice(user_agents) if isinstance(user_agents, list) and user_agents else DEFAULT_USER_AGENT
        logger.info(
            "component=kick_viewer event=initialized viewer=%s channel_id=%s chatroom_id=%s ua=%s",
            self.viewer_id,
            self.channel_id,
            self.chatroom_id,
            self.user_agent,
        )

    async def get_viewer_token(self, session: aiohttp.ClientSession) -> str:
        session_token = str(self.config.get("session_token", "")).strip()
        if not session_token:
            raise RuntimeError("session_token is required to get viewer token")

        client_token = self.config.get("viewer_token", "") or self.config.get("chat_token", "")
        if not client_token:
            raise RuntimeError("viewer_token is required to get viewer token")

        user_agents = self.config.get("user_agents")
        request_user_agent = (
            str(user_agents[0])
            if isinstance(user_agents, list) and user_agents
            else self.user_agent or DEFAULT_USER_AGENT
        )
        url = "https://websockets.kick.com/viewer/v1/token"
        headers = {
            "Authorization": f"Bearer {session_token}",
            "x-client-token": client_token,
            "x-app-platform": "web",
            "origin": "https://kick.com",
            "referer": "https://kick.com/",
            "User-Agent": request_user_agent,
        }
        backoff = 1
        for _ in range(5):
            try:
                logger.info("viewer=%s requesting viewer token", self.viewer_id)
                payload = await asyncio.to_thread(self._get_viewer_token_payload, url, headers)
                viewer_token = str(payload.get("token", ""))
                if viewer_token:
                    logger.info("Obtained viewer token using session_token")
                    return viewer_token
                raise RuntimeError("empty viewer token")
            except Exception as exc:
                logger.warning("viewer=%s get_viewer_token error: %s", self.viewer_id, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
        raise RuntimeError("cannot get viewer token")

    @staticmethod
    def _get_viewer_token_payload(url: str, headers: dict) -> dict:
        curl_session = curl_requests.Session()
        try:
            response = curl_session.get(
                url,
                impersonate="chrome124",
                headers=headers,
                timeout=10,
            )
            if response.status_code != 200:
                response.raise_for_status()
                raise RuntimeError(f"unexpected viewer token status: {response.status_code}")
            return response.json()
        finally:
            curl_session.close()

    async def run(self) -> None:
        async with aiohttp.ClientSession() as session:
            chat_task = None
            if self.config.get("chat_token", ""):
                chat_task = asyncio.create_task(self._chat_reconnect_loop(self.chatroom_id))
            reconnect_attempt = 0
            reconnect_delay = self._reconnect_base_delay
            if self._startup_jitter_max > 0:
                await asyncio.sleep(random.uniform(0, self._startup_jitter_max))
            while self._running:
                try:
                    viewer_token = await self.get_viewer_token(session)
                    logger.info("viewer=%s connecting to WS", self.viewer_id)
                    await self._connect_viewer_loop(self.channel_id, viewer_token)
                    reconnect_attempt = 0
                    reconnect_delay = self._reconnect_base_delay
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    reconnect_attempt += 1
                    if self._max_reconnect_attempts and reconnect_attempt > self._max_reconnect_attempts:
                        logger.error(
                            "viewer=%s ws reconnect attempts exceeded max=%s",
                            self.viewer_id,
                            self._max_reconnect_attempts,
                        )
                        break
                    logger.warning(
                        "viewer=%s ws reconnect attempt=%s delay=%.1fs error=%s",
                        self.viewer_id,
                        reconnect_attempt,
                        reconnect_delay,
                        exc,
                    )
                    await asyncio.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, self._reconnect_max_delay)
            if chat_task:
                chat_task.cancel()
                with contextlib.suppress(Exception):
                    await chat_task

    async def _connect_viewer_loop(self, channel_id: str, viewer_token: str) -> None:
        async with websockets.connect(
            self._ws_uri,
            ping_interval=None,
            max_queue=1,
            compression=None,
            close_timeout=3,
            additional_headers={"User-Agent": self.user_agent},
        ) as ws:
            self._ws = ws
            if self._on_ws_connection_change:
                self._on_ws_connection_change(1)
            await ws.send(
                json.dumps(
                    {
                        "event": "pusher:subscribe",
                        "data": {
                            "channel": f"livestream.{channel_id}.viewers",
                            "auth": viewer_token,
                        },
                    }
                )
            )
            try:
                async for message in ws:
                    await self._handle_ws_message(ws, message)
            finally:
                if self._on_ws_connection_change:
                    self._on_ws_connection_change(-1)
                self._ws = None

    async def _connect_chat_loop(self, chatroom_id: str) -> None:
        uri = "wss://ws-us2.pusher.com/app/32cbd69e4b950bf97679"
        async with websockets.connect(
            uri,
            ping_interval=None,
            additional_headers={"User-Agent": self.user_agent},
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
                logger.warning("viewer=%s chat ws reconnect after error: %s", self.viewer_id, exc)
                await asyncio.sleep(3)

    async def _handle_ws_message(self, ws, raw_message: str) -> None:
        try:
            payload = json.loads(raw_message)
        except Exception:
            return
        if payload.get("event") == "pusher:ping":
            await ws.send(json.dumps({"event": "pusher:pong", "data": {}}))

    async def stop(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()
