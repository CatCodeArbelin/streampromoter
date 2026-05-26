import asyncio
import contextlib
import json
import logging
import os
from pathlib import Path

import aiohttp

from kick_promoter.ai_bot.chat_poster import ChatPoster
from kick_promoter.ai_bot.openai_client import OpenAIClient
from kick_promoter.viewer.viewer_pool import ViewerPool

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
logger = logging.getLogger(__name__)


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


def _env_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def load_config(path: str = "kick_promoter/config.json") -> dict:
    config = json.loads(Path(path).read_text(encoding="utf-8"))

    secrets_path = Path("kick_promoter/secrets.json")
    if secrets_path.exists():
        try:
            secrets_config = json.loads(secrets_path.read_text(encoding="utf-8"))
            if isinstance(secrets_config, dict):
                config.update(secrets_config)
        except json.JSONDecodeError:
            logger.warning("component=config event=invalid_secrets_json path=%s", secrets_path)

    str_map = {
        "KICK_CHANNEL": "kick_channel",
        "KICK_CHATROOM_ID": "kick_chatroom_id",
        "CHAT_TOKEN": "chat_token",
        "OPENAI_API_KEY": "openai_api_key",
        "OPENAI_MODEL": "openai_model",
        "OPENAI_VOICE": "openai_voice",
        "WEB_HOST": "web_host",
    }
    int_map = {
        "VIEWER_COUNT": "viewer_count",
        "VIEWER_PING_INTERVAL_SEC": "viewer_ping_interval_sec",
        "POST_INTERVAL_SEC": "post_interval_sec",
        "OPENAI_THROTTLE_SEC": "openai_throttle_sec",
        "WEB_PORT": "web_port",
    }

    for env_name, cfg_key in str_map.items():
        if os.getenv(env_name):
            config[cfg_key] = os.getenv(env_name)

    for env_name, cfg_key in int_map.items():
        if os.getenv(env_name):
            config[cfg_key] = int(os.getenv(env_name, "0"))

    if os.getenv("OPENAI_ENABLED") is not None:
        config["openai_enabled"] = _env_bool(os.getenv("OPENAI_ENABLED", "false"))

    return config


class Runner:
    def __init__(self, config: dict):
        self.config = config
        self._stop_event = asyncio.Event()
        self._started = False
        self._status = "stopped"
        self._session = None
        self._viewer_pool = None
        self._openai_client = None
        self._openai_task = None

    def status(self) -> dict:
        return {
            "status": self._status,
            "started": self._started,
            "stopping": self._stop_event.is_set(),
        }

    async def start(self) -> None:
        if self._started:
            logger.info("component=runner event=start_skip reason=already_started")
            return

        self._started = True
        self._status = "running"
        timeout_sec = int(self.config.get("post_timeout_sec", 10))
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout_sec))
        poster = ChatPoster(self.config, session=self._session)
        self._viewer_pool = ViewerPool(self.config)
        self._openai_client = OpenAIClient(config=self.config, chat_poster=poster)

        logger.info("component=runner event=start")
        logger.info("component=viewer_pool event=start")
        await self._viewer_pool.start()

        logger.info("component=openai_client event=start")
        self._openai_task = asyncio.create_task(self._openai_client.run(), name="openai-client")

        waiter = asyncio.create_task(self._stop_event.wait(), name="runner-stop-waiter")
        viewer_wait = asyncio.create_task(self._viewer_pool.wait(), name="viewer-pool-wait")
        done, pending = await asyncio.wait({waiter, viewer_wait, self._openai_task}, return_when=asyncio.FIRST_COMPLETED)

        if waiter in done:
            logger.info("component=runner event=stop_event_received")
        elif viewer_wait in done:
            logger.warning("component=viewer_pool event=completed_or_failed")
        elif self._openai_task in done:
            logger.warning("component=openai_client event=completed_or_failed")

        for task in pending:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        await self.stop()

    async def stop(self) -> None:
        if self._status in {"stopped", "stopping"}:
            return

        self._status = "stopping"
        self._stop_event.set()
        stop_timeout = float(self.config.get("shutdown_timeout_sec", 10))
        logger.info("component=runner event=stop timeout=%s", stop_timeout)

        if self._viewer_pool:
            logger.info("component=viewer_pool event=stop")
            await self._viewer_pool.stop()

        if self._openai_client:
            logger.info("component=openai_client event=stop")
            await self._openai_client.stop()

        if self._openai_task:
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(self._openai_task), timeout=stop_timeout)
            if not self._openai_task.done():
                self._openai_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._openai_task

        if self._session:
            await self._session.close()

        self._status = "stopped"
        logger.info("component=runner event=stopped")


async def run_bot(config: dict) -> None:
    runner = Runner(config)
    await runner.start()


def main() -> None:
    setup_logging()
    config = load_config()
    try:
        asyncio.run(run_bot(config))
    except KeyboardInterrupt:
        logging.info("Stopped by user")


if __name__ == "__main__":
    main()
