import asyncio
import logging
import random
from collections.abc import Callable
from pathlib import Path

import aiohttp

logger = logging.getLogger(__name__)

MAX_CHAT_MESSAGE_LENGTH = 500
MIN_CHAT_TOKEN_LENGTH = 10


class ChatPostValidator:
    """Validation layer extracted for unit-level testing and extension."""

    def __init__(self, max_text_length: int = MAX_CHAT_MESSAGE_LENGTH, min_token_length: int = MIN_CHAT_TOKEN_LENGTH):
        self.max_text_length = max_text_length
        self.min_token_length = min_token_length

    def validate(self, *, text: str, chatroom_id: str | None, token: str | None) -> tuple[bool, str | None]:
        text_value = text if isinstance(text, str) else ""
        chatroom_value = self._sanitize_string(chatroom_id)
        token_value = self._sanitize_string(token)

        if not text_value:
            return False, "empty message text"

        if len(text_value) > self.max_text_length:
            return False, f"message too long len={len(text_value)} limit={self.max_text_length}"

        if not chatroom_value:
            logger.warning("Chat posting disabled: missing kick_chatroom_id")
            return True, None

        if not chatroom_value.isdigit():
            logger.warning("Chat posting disabled: invalid kick_chatroom_id format=%r", chatroom_value)
            return True, None

        if not token_value:
            logger.warning("Chat posting disabled: missing chat_token")
            return True, None

        if len(token_value) < self.min_token_length:
            logger.warning("Chat posting disabled: chat_token too short len=%s min=%s", len(token_value), self.min_token_length)
            return True, None

        return True, None

    @staticmethod
    def _sanitize_string(value: str | None) -> str:
        return value.strip() if isinstance(value, str) else ""


class ChatPoster:
    def __init__(
        self,
        config: dict,
        session: aiohttp.ClientSession,
        telemetry_callback: Callable[..., None] | None = None,
        validator: ChatPostValidator | None = None,
    ):
        self.config = config
        self.session = session
        runtime_phrases = config.get("runtime_phrases") or []
        if runtime_phrases:
            self.phrases = [phrase.strip() for phrase in runtime_phrases if str(phrase).strip()]
        else:
            self.phrases = [
                phrase.strip()
                for phrase in Path("kick_promoter/phrases.txt").read_text(encoding="utf-8").splitlines()
                if phrase.strip()
            ]
        self._telemetry_callback = telemetry_callback
        self._validator = validator or ChatPostValidator()
        self._skip_probability = float(self.config.get("chat_idle_skip_probability", 0.1))
        self._interval_mean_sec = float(self.config.get("chat_interval_mean_sec", self.config.get("post_interval_sec", 30)))
        self._interval_std_sec = float(self.config.get("chat_interval_std_sec", 8))
        self._chat_jitter_max = float(self.config.get("chat_jitter_max", 0))

    def should_skip_cycle(self) -> bool:
        probability = min(max(self._skip_probability, 0.0), 1.0)
        return random.random() < probability

    def compute_next_delay(self, *, min_delay: float) -> float:
        base_delay = max(min_delay, random.gauss(self._interval_mean_sec, self._interval_std_sec))
        jitter = random.uniform(0, self._chat_jitter_max) if self._chat_jitter_max > 0 else 0.0
        return base_delay + jitter

    async def post(self, text: str) -> None:
        chatroom_id = self.config.get("kick_chatroom_id")
        token = self.config.get("chat_token", "")
        if not str(token).strip():
            logger.warning("Chat posting skipped: no chat_token")
            return None

        is_valid, reason = self._validator.validate(text=text, chatroom_id=chatroom_id, token=token)
        if not is_valid:
            logger.warning("Skip post: invalid payload reason=%s", reason)
            return

        url = f"https://kick.com/api/v2/messages/send/{str(chatroom_id).strip()}"
        headers = {"Authorization": f"Bearer {str(token).strip()}"}

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
                        if self.config.get("openai_enabled") and self._telemetry_callback:
                            self._telemetry_callback(ai_message=text)
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
            if self.should_skip_cycle():
                logger.debug("component=chat_poster event=skip_cycle reason=bernoulli probability=%.3f", self._skip_probability)
                await asyncio.sleep(self.compute_next_delay(min_delay=1.0))
                continue

            await self.post(random.choice(self.phrases))
            delay = self.compute_next_delay(min_delay=float(self.config.get("post_interval_sec", 30)))
            logger.debug("component=chat_poster event=post_delay delay_sec=%.3f", delay)
            await asyncio.sleep(delay)
