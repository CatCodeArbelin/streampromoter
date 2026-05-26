import asyncio
import json
import logging
from pathlib import Path

from kick_promoter.ai_bot.chat_poster import ChatPoster
from kick_promoter.ai_bot.openai_client import OpenAIClient
from kick_promoter.viewer.viewer_pool import ViewerPool

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


def load_config(path: str = "kick_promoter/config.json") -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


async def run_bot(config: dict) -> None:
    poster = ChatPoster(config)
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
