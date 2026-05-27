import asyncio
import contextlib
import json
import logging
import os
import socket
from pathlib import Path

import aiohttp
from curl_cffi import requests

from kick_promoter.ai_bot.chat_poster import ChatPoster
from kick_promoter.ai_bot.openai_client import OpenAIClient
from kick_promoter.token_validator import validate_x_client_token
from kick_promoter.viewer.viewer_pool import ViewerPool

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | node_id=%(node_id)s | %(message)s"
logger = logging.getLogger(__name__)


def resolve_node_id() -> str:
    env_node_id = str(os.getenv("NODE_ID", "")).strip()
    if env_node_id:
        return env_node_id

    hostname = str(os.getenv("HOSTNAME", "")).strip()
    if hostname:
        return hostname

    return socket.gethostname()


class NodeContextFilter(logging.Filter):
    def __init__(self, node_id: str):
        super().__init__()
        self.node_id = node_id

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "node_id"):
            record.node_id = self.node_id
        return True


def setup_logging(node_id: str) -> None:
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    logging.getLogger().addFilter(NodeContextFilter(node_id))


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
    def __init__(self, config: dict, telemetry_callback=None):
        self.config = config
        self._stop_event = asyncio.Event()
        self._started = False
        self._status = "stopped"
        self._session = None
        self._viewer_pool = None
        self._openai_client = None
        self._openai_task = None
        self._telemetry_callback = telemetry_callback
        self._viewer_pool_stop_timeout_sec = float(self.config.get("viewer_pool_stop_timeout_sec", 15))
        self._openai_stop_timeout_sec = float(self.config.get("openai_stop_timeout_sec", 10))

    def status(self) -> dict:
        return {
            "status": self._status,
            "started": self._started,
            "stopping": self._stop_event.is_set(),
        }

    async def _assert_channel_live(self) -> None:
        channel = str(self.config.get("kick_channel", "")).strip()
        if not channel:
            raise RuntimeError("kick_channel is required")

        url = f"https://kick.com/api/v2/channels/{channel}"

        def _fetch_channel_data() -> dict:
            session = requests.Session()
            response = session.get(url, impersonate="chrome110")
            if response.status_code != 200:
                raise RuntimeError("Channel is not live. Load test aborted.")
            data = response.json()
            return data if isinstance(data, dict) else {}

        payload = await asyncio.to_thread(_fetch_channel_data)

        livestream = payload.get("livestream") or {}
        if livestream.get("is_live") is not True:
            raise RuntimeError("Channel is not live. Load test aborted.")

    async def _run_openai_with_restarts(self) -> None:
        backoff_sec = 1.0
        max_backoff_sec = float(self.config.get("openai_restart_backoff_max_sec", 30))

        while self._status == "running" and not self._stop_event.is_set():
            try:
                await self._openai_client.run()
                if self._stop_event.is_set() or self._status != "running":
                    break
                logger.warning("component=openai_client event=run_completed_unexpectedly action=restart")
            except asyncio.CancelledError:
                raise
            except Exception:
                if self._stop_event.is_set() or self._status != "running":
                    break
                logger.exception(
                    "component=openai_client event=run_failed action=restart backoff_sec=%.1f",
                    backoff_sec,
                )

            await asyncio.sleep(backoff_sec)
            backoff_sec = min(backoff_sec * 2, max_backoff_sec)

    async def start(self) -> None:
        try:
            if self._status == "running":
                logger.info("component=runner event=start_skip reason=already_running")
                return
            if self._status == "stopping" or self._stop_event.is_set():
                logger.info("component=runner event=start_skip reason=stopping")
                return
            if self._started:
                logger.info("component=runner event=start_skip reason=already_started")
                return

            self._started = True
            self._status = "running"
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10, connect=5, sock_read=5)
            )
            poster = ChatPoster(self.config, session=self._session, telemetry_callback=self._telemetry_callback)
            self._viewer_pool = ViewerPool(self.config, telemetry_callback=self._telemetry_callback)
            self._openai_client = OpenAIClient(config=self.config, chat_poster=poster)

            logger.info("component=runner event=start")
            logger.info(
                "component=runner event=checking_channel_live channel=%s",
                self.config.get("kick_channel"),
            )
            logger.info("component=runner event=assert_channel_live")
            await self._assert_channel_live()
            logger.info("component=token_validator event=validate_start")
            await validate_x_client_token(self.config, self._session)
            logger.info("component=viewer_pool event=start")
            await self._viewer_pool.start()

            logger.info("component=openai_client event=start")
            self._openai_task = asyncio.create_task(self._run_openai_with_restarts(), name="openai-watchdog")

            waiter = asyncio.create_task(self._stop_event.wait(), name="runner-stop-waiter")
            viewer_wait = asyncio.create_task(self._viewer_pool.wait(), name="viewer-pool-wait")
            done, pending = await asyncio.wait({waiter, viewer_wait}, return_when=asyncio.FIRST_COMPLETED)

            if waiter in done:
                logger.info("component=runner event=stop_event_received")
            elif viewer_wait in done:
                logger.warning("component=viewer_pool event=completed_or_failed")

            for task in pending:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

            await self.stop()
        except Exception:
            logger.exception("Runner start failed")
            raise

    async def stop(self) -> None:
        if self._status == "stopped":
            logger.info("component=runner event=stop_skip reason=already_stopped")
            return
        if self._status == "stopping":
            logger.info("component=runner event=stop_skip reason=already_stopping")
            return

        self._status = "stopping"
        self._stop_event.set()
        stop_timeout = float(self.config.get("shutdown_timeout_sec", 10))
        logger.info("component=runner event=stop timeout=%s", stop_timeout)

        if self._viewer_pool:
            logger.info("component=viewer_pool event=stop timeout=%s", self._viewer_pool_stop_timeout_sec)
            try:
                await asyncio.wait_for(self._viewer_pool.stop(), timeout=self._viewer_pool_stop_timeout_sec)
            except asyncio.TimeoutError:
                logger.warning("component=viewer_pool event=stop_timeout timeout=%s", self._viewer_pool_stop_timeout_sec)

        if self._openai_client:
            logger.info("component=openai_client event=stop timeout=%s", self._openai_stop_timeout_sec)
            try:
                await asyncio.wait_for(self._openai_client.stop(), timeout=self._openai_stop_timeout_sec)
            except asyncio.TimeoutError:
                logger.warning("component=openai_client event=stop_timeout timeout=%s", self._openai_stop_timeout_sec)

        if self._openai_task:
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(self._openai_task), timeout=stop_timeout)
            if not self._openai_task.done():
                self._openai_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._openai_task

        if self._session:
            await self._session.close()

        self._started = False
        self._status = "stopped"
        logger.info("component=runner event=stopped")


async def run_bot(config: dict) -> None:
    runner = Runner(config)
    await runner.start()


def main() -> None:
    node_id = resolve_node_id()
    setup_logging(node_id=node_id)
    config = load_config()
    config.setdefault("node_id", node_id)
    try:
        asyncio.run(run_bot(config))
    except KeyboardInterrupt:
        logging.info("Stopped by user")


if __name__ == "__main__":
    main()
