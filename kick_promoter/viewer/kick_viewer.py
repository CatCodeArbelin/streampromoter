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

    async def resolve_channel_id(self, session: aiohttp.ClientSession) -> str:
        channel = self.config["kick_channel"]
        url = f"https://kick.com/api/v2/channels/{channel}"
        backoff = 1
        for _ in range(5):
            try:
                async with session.get(url, timeout=10) as response:
                    response.raise_for_status()
                    payload = await response.json()
                    return str(payload.get("id") or payload.get("chatroom", {}).get("id", ""))
            except Exception as exc:
                logger.warning("viewer=%s resolve_channel_id error: %s", self.viewer_id, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
        raise RuntimeError("cannot resolve channel id")

    async def run(self) -> None:
        async with aiohttp.ClientSession() as session:
            channel_id = await self.resolve_channel_id(session)
            while self._running:
                try:
                    await self._connect_and_loop(channel_id)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning("viewer=%s ws reconnect after error: %s", self.viewer_id, exc)
                    await asyncio.sleep(3)

    async def _connect_and_loop(self, channel_id: str) -> None:
        uri = "wss://ws-us2.pusher.com/app/32cbd69e4b950bf97679"
        async with websockets.connect(uri, ping_interval=None) as ws:
            self._ws = ws
            await ws.send(
                json.dumps(
                    {
                        "event": "pusher:subscribe",
                        "data": {"channel": f"chatrooms.{channel_id}.v2"},
                    }
                )
            )
            ping_interval = int(self.config.get("viewer_ping_interval_sec", 20))
            ping_task = asyncio.create_task(self._ping_loop(ws, ping_interval))
            try:
                async for message in ws:
                    logger.debug("viewer=%s msg=%s", self.viewer_id, message)
            finally:
                ping_task.cancel()
                with contextlib.suppress(Exception):
                    await ping_task

    async def _ping_loop(self, ws, interval: int) -> None:
        while self._running:
            await asyncio.sleep(interval)
            await ws.send(json.dumps({"event": "pusher:ping", "data": {}}))

    async def stop(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()
