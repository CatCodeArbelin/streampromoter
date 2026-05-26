import asyncio
import logging
import random

import aiohttp

logger = logging.getLogger(__name__)


class ChatPoster:
    def __init__(self, config: dict):
        self.config = config
        self.phrases = [p.strip() for p in open("kick_promoter/phrases.txt", encoding="utf-8") if p.strip()]

    async def post(self, text: str) -> None:
        chatroom_id = self.config.get("kick_chatroom_id")
        if not chatroom_id:
            logger.info("Mock post: %s", text)
            return
        url = f"https://kick.com/api/v2/messages/send/{chatroom_id}"
        backoff = 1
        async with aiohttp.ClientSession() as session:
            for _ in range(5):
                try:
                    async with session.post(url, json={"content": text}, timeout=10) as response:
                        response.raise_for_status()
                        return
                except Exception as exc:
                    logger.warning("post retry on error=%s", exc)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 30)

    async def fallback_loop(self) -> None:
        while True:
            await self.post(random.choice(self.phrases))
            await asyncio.sleep(int(self.config.get("post_interval_sec", 30)))
