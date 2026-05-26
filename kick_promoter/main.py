import asyncio
import json
import logging
import os
from pathlib import Path

import aiohttp

from kick_promoter.ai_bot.chat_poster import ChatPoster
from kick_promoter.ai_bot.openai_client import OpenAIClient
from kick_promoter.viewer.viewer_pool import ViewerPool

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


def _env_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def load_config(path: str = "kick_promoter/config.json") -> dict:
    config = json.loads(Path(path).read_text(encoding="utf-8"))

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


async def run_bot(config: dict) -> None:
    timeout_sec = int(config.get("post_timeout_sec", 10))
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout_sec)) as session:
        poster = ChatPoster(config, session=session)
        viewer_pool = ViewerPool(config)
        await viewer_pool.start()

        openai_client = OpenAIClient(config=config, chat_poster=poster)
        openai_task = asyncio.create_task(openai_client.run(), name="openai-client")

        try:
            await asyncio.gather(viewer_pool.wait(), openai_task)
        finally:
            await viewer_pool.stop()
            await openai_client.stop()


def main() -> None:
    setup_logging()
    config = load_config()
    try:
        asyncio.run(run_bot(config))
    except KeyboardInterrupt:
        logging.info("Stopped by user")


if __name__ == "__main__":
    main()
