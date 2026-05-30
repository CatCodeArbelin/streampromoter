import asyncio
import contextlib
import json
import logging
import os
import socket
from pathlib import Path

import aiohttp
from curl_cffi import requests as curl_requests

from kick_promoter.ai_bot.chat_poster import ChatPoster
from kick_promoter.ai_bot.openai_client import OpenAIClient
from kick_promoter.kick_http_client import KickHttpClient
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
        "SESSION_TOKEN": "session_token",
        "OPENAI_API_KEY": "openai_api_key",
        "GOOGLE_API_KEY": "google_api_key",
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
        self._kick_http_client = KickHttpClient(config)
        self._viewer_pool_stop_timeout_sec = float(self.config.get("viewer_pool_stop_timeout_sec", 15))
        self._openai_stop_timeout_sec = float(self.config.get("openai_stop_timeout_sec", 10))

    def status(self) -> dict:
        return {
            "status": self._status,
            "started": self._started,
            "stopping": self._stop_event.is_set(),
        }

    def _build_kick_channel_headers(self, channel: str) -> dict[str, str]:
        user_agents = self.config.get("user_agents") or []
        user_agent = str(user_agents[0]).strip() if user_agents else ""
        if not user_agent:
            user_agent = (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/136.0.0.0 Safari/537.36"
            )

        headers = {
            "User-Agent": user_agent,
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://kick.com",
            "Referer": f"https://kick.com/{channel}",
        }

        session_token = str(self.config.get("session_token", "")).strip()
        if session_token:
            headers["Authorization"] = f"Bearer {session_token}"

        x_client_token = str(
            self.config.get("x_client_token") or self.config.get("viewer_token") or ""
        ).strip()
        if x_client_token:
            headers["x-client-token"] = x_client_token

        return headers

    def _fetch_channel_data(self, channel: str) -> dict:
        url = f"https://kick.com/api/v2/channels/{channel}"
        session = curl_requests.Session()
        try:
            response = session.get(
                url,
                headers=self._build_kick_channel_headers(channel),
                impersonate="chrome124",
            )
            response.raise_for_status()
            payload = response.json()
            return payload if isinstance(payload, dict) else {}
        finally:
            session.close()

    async def _assert_channel_live(self) -> None:
        channel = str(self.config.get("kick_channel", "")).strip()
        if not channel:
            raise RuntimeError("kick_channel is required")

        try:
            payload = await asyncio.to_thread(self._fetch_channel_data, channel)
        except Exception as exc:
            raise RuntimeError("Channel is not live. Load test aborted.") from exc

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

            logger.info("component=runner event=start")
            try:
                logger.info(
                    "component=runner event=checking_channel_live channel=%s",
                    self.config.get("kick_channel"),
                )
                logger.info("component=runner event=assert_channel_live")
                await self._assert_channel_live()
            except Exception:
                logger.exception("component=runner step=assert_channel_live event=failed")
                raise

            try:
                self._session = aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=10, connect=5, sock_read=5)
                )
                self._viewer_pool = ViewerPool(self.config, telemetry_callback=self._telemetry_callback)

                logger.info("component=token_validator event=validate_start")
                validated_token = await validate_x_client_token(self.config, self._kick_http_client)
                if validated_token is None:
                    logger.warning("Could not auto-detect viewer token, using value from config")
            except Exception:
                logger.exception("component=runner step=viewer_pool_init event=failed")
                raise

            try:
                logger.info("component=viewer_pool event=start")
                await self._viewer_pool.start()
            except Exception:
                logger.exception("component=runner step=viewer_pool_start event=failed")
                raise

            openai_enabled = bool(self.config.get("openai_enabled"))
            if openai_enabled:
                try:
                    chat_token = str(self.config.get("chat_token", "")).strip()
                    if not chat_token:
                        raise RuntimeError("openai_enabled=true requires non-empty chat_token")

                    poster = ChatPoster(
                        self.config,
                        session=self._session,
                        telemetry_callback=self._telemetry_callback,
                    )
                    self._openai_client = OpenAIClient(config=self.config, chat_poster=poster)

                    logger.info("component=openai_client event=start")
                    self._openai_task = asyncio.create_task(
                        self._run_openai_with_restarts(),
                        name="openai-watchdog",
                    )
                except Exception:
                    logger.exception("component=runner step=openai_init event=failed")
                    raise
            else:
                logger.info("component=openai_client event=skip reason=openai_disabled")

            waiter = asyncio.create_task(self._stop_event.wait(), name="runner-stop-waiter")
            viewer_wait = self._viewer_pool.wait()
            try:
                await asyncio.gather(waiter, viewer_wait)
            except asyncio.CancelledError:
                pass

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
