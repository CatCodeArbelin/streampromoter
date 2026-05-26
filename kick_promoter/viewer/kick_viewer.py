import asyncio
import json
import logging
import contextlib
from typing import Optional

import aiohttp
import websockets

logger = logging.getLogger(__name__)


class KickViewer:
    def __init__(self, config: dict, viewer_id: int):
        self.config = config
        self.viewer_id = viewer_id
        self._running = True
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._ws_uri = "wss://websockets.kick.com/viewer/v1/connect"

    async def resolve_channel_id(self, session: aiohttp.ClientSession) -> str:
        channel = self.config["kick_channel"]
        url = f"https://kick.com/api/v2/channels/{channel}"
        backoff = 1
        for _ in range(5):
            try:
                async with session.get(url, timeout=10) as response:
                    response.raise_for_status()
                    payload = await response.json()
                    channel_id = str(payload.get("id", ""))
                    if channel_id:
                        return channel_id
                    raise RuntimeError("empty channel id")
            except Exception as exc:
                logger.warning("viewer=%s resolve_channel_id error: %s", self.viewer_id, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
        raise RuntimeError("cannot resolve channel id")

    async def resolve_chatroom_id(self, session: aiohttp.ClientSession) -> str:
        channel = self.config["kick_channel"]
        url = f"https://kick.com/api/v2/channels/{channel}/chatroom"
        backoff = 1
        for _ in range(5):
            try:
                async with session.get(url, timeout=10) as response:
                    response.raise_for_status()
                    payload = await response.json()
                    chatroom_id = str(payload.get("id", ""))
                    if chatroom_id:
                        return chatroom_id
                    raise RuntimeError("empty chatroom id")
            except Exception as exc:
                logger.warning("viewer=%s resolve_chatroom_id error: %s", self.viewer_id, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
        raise RuntimeError("cannot resolve chatroom id")

    async def get_viewer_token(self, session: aiohttp.ClientSession) -> str:
        client_token = self.config.get("chat_token", "")
        if not client_token:
            raise RuntimeError("chat_token is required to get viewer token")

        url = "https://websockets.kick.com/viewer/v1/token"
        headers = {"X-CLIENT-TOKEN": client_token}
        backoff = 1
        for _ in range(5):
            try:
                async with session.get(url, headers=headers, timeout=10) as response:
                    response.raise_for_status()
                    payload = await response.json()
                    viewer_token = str(payload.get("token", ""))
                    if viewer_token:
                        return viewer_token
                    raise RuntimeError("empty viewer token")
            except Exception as exc:
                logger.warning("viewer=%s get_viewer_token error: %s", self.viewer_id, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
        raise RuntimeError("cannot get viewer token")

    async def run(self) -> None:
        async with aiohttp.ClientSession() as session:
            channel_id = await self.resolve_channel_id(session)
            chatroom_id = await self.resolve_chatroom_id(session)
            logger.debug(
                "viewer=%s resolved channel_id=%s chatroom_id=%s",
                self.viewer_id,
                channel_id,
                chatroom_id,
            )
            chat_task = None
            if self.config.get("chat_token", ""):
                chat_task = asyncio.create_task(self._chat_reconnect_loop(chatroom_id))
            while self._running:
                try:
                    viewer_token = await self.get_viewer_token(session)
                    await self._connect_viewer_loop(channel_id, viewer_token)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning("viewer=%s ws reconnect after error: %s", self.viewer_id, exc)
                    await asyncio.sleep(3)
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
        ) as ws:
            self._ws = ws
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
                self._ws = None

    async def _connect_chat_loop(self, chatroom_id: str) -> None:
        uri = "wss://ws-us2.pusher.com/app/32cbd69e4b950bf97679"
        async with websockets.connect(uri, ping_interval=None) as ws:
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
