import asyncio
import logging

logger = logging.getLogger(__name__)


class TokenRateLimiter:
    def __init__(self, delay_seconds: float = 0.8):
        if delay_seconds <= 0:
            raise ValueError("delay_seconds must be non-zero and positive")
        self._semaphore = asyncio.Semaphore(1)
        self._delay_seconds = float(delay_seconds)

    async def acquire(self) -> None:
        async with self._semaphore:
            logger.info("token_limiter: allowing request")
            await asyncio.sleep(self._delay_seconds)
            logger.info("token_limiter: waiting finished")
