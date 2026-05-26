import asyncio
import logging
import random
from pathlib import Path

import aiohttp

logger = logging.getLogger(__name__)


class ChatPoster:
    def __init__(self, config: dict, session: aiohttp.ClientSession):
        self.config = config
        self.session = session
        self.phrases = [
            phrase.strip()
            for phrase in Path("kick_promoter/phrases.txt").read_text(encoding="utf-8").splitlines()
            if phrase.strip()
        ]

    async def post(self, text: str) -> None:
        chatroom_id = self.config.get("kick_chatroom_id")
        token = self.config.get("chat_token", "")
        if not chatroom_id or not token:
            logger.warning("Skip post: missing chatroom_id or chat_token")
            return

        url = f"https://kick.com/api/v2/messages/send/{chatroom_id}"
        headers = {"Authorization": f"Bearer {token}"}

        max_retries = int(self.config.get("post_max_retries", 5))
        timeout_sec = int(self.config.get("post_timeout_sec", 10))
        backoff = 1

        for attempt in range(1, max_retries + 1):
            try:
                async with self.session.post(
                    url,
                    json={"content": text, "type": "message"},
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=timeout_sec),
                ) as response:
                    if response.status < 400:
                        return

                    if response.status == 429 or response.status >= 500:
                        raise aiohttp.ClientResponseError(
                            response.request_info,
                            response.history,
                            status=response.status,
                            message=f"Retryable response status={response.status}",
                            headers=response.headers,
                        )

                    logger.warning("Non-retryable post error status=%s", response.status)
                    return
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt >= max_retries:
                    logger.warning("Post failed after retries error=%s", exc)
                    return

                logger.warning("Post retry %s/%s on error=%s", attempt, max_retries, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    async def fallback_loop(self) -> None:
        while True:
            await self.post(random.choice(self.phrases))
            await asyncio.sleep(int(self.config.get("post_interval_sec", 30)))
