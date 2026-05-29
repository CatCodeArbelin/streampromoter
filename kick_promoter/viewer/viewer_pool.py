import asyncio
import logging
import random
from collections.abc import Callable

from curl_cffi import requests

from kick_promoter.viewer.kick_viewer import KickViewer

logger = logging.getLogger(__name__)


class ViewerPool:
    def __init__(self, config: dict, telemetry_callback: Callable[..., None] | None = None):
        self.config = config
        self.tasks: list[asyncio.Task] = []
        self.viewers: list[KickViewer] = []
        self._status_task: asyncio.Task | None = None
        self._running = False
        self._telemetry_callback = telemetry_callback
        self._started_workers = 0
        self._target_workers = 0
        self._active_ws_connections = 0
        self._channel_id: str | None = None
        self._chatroom_id: str | None = None

    def _emit_telemetry(self) -> None:
        if self._telemetry_callback:
            self._telemetry_callback(
                started_workers=self._started_workers,
                target_workers=self._target_workers,
                active_ws_connections=self._active_ws_connections,
            )

    def _on_ws_connection_change(self, delta: int) -> None:
        self._active_ws_connections = max(0, self._active_ws_connections + delta)
        self._emit_telemetry()

    async def _resolve_channel_id(self) -> str:
        channel = str(self.config.get("kick_channel", "")).strip()
        if not channel:
            raise RuntimeError("kick_channel is required")

        url = f"https://kick.com/api/v2/channels/{channel}"
        backoff = 1
        for _ in range(5):
            try:
                def _sync_req() -> str:
                    session = requests.Session()
                    try:
                        headers = {
                            "User-Agent": self.config["user_agents"][0],
                            "Accept": "application/json",
                            "Accept-Language": "en-US,en;q=0.9",
                            "Origin": "https://kick.com",
                            "Referer": "https://kick.com/",
                        }
                        session_token = str(self.config.get("session_token", "")).strip()
                        if session_token:
                            headers["Authorization"] = f"Bearer {session_token}"
                        viewer_token = str(self.config.get("viewer_token", "")).strip()
                        if viewer_token:
                            headers["x-client-token"] = viewer_token

                        resp = session.get(url, headers=headers, impersonate="chrome124")
                        if resp.status_code != 200:
                            raise RuntimeError(f"unexpected channel status: {resp.status_code}")
                        channel_id = str(resp.json().get("id", "")).strip()
                        if channel_id:
                            return channel_id
                        raise RuntimeError("empty channel id")
                    finally:
                        session.close()

                channel_id = await asyncio.to_thread(_sync_req)
                return channel_id
            except Exception as exc:
                logger.warning("component=viewer_pool event=resolve_channel_id_failed error=%s", exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
        raise RuntimeError("cannot resolve channel id")

    async def _resolve_chatroom_id(self, channel: str) -> str:
        configured_chatroom_id = str(self.config.get("kick_chatroom_id", "")).strip()
        if configured_chatroom_id:
            return configured_chatroom_id

        url = f"https://kick.com/api/v2/channels/{channel}/chatroom"
        backoff = 1
        for _ in range(5):
            try:
                def _sync_req() -> str:
                    session = requests.Session()
                    try:
                        headers = {
                            "User-Agent": self.config["user_agents"][0],
                            "Accept": "application/json",
                            "Accept-Language": "en-US,en;q=0.9",
                            "Origin": "https://kick.com",
                            "Referer": "https://kick.com/",
                        }
                        session_token = str(self.config.get("session_token", "")).strip()
                        if session_token:
                            headers["Authorization"] = f"Bearer {session_token}"
                        viewer_token = str(self.config.get("viewer_token", "")).strip()
                        if viewer_token:
                            headers["x-client-token"] = viewer_token

                        resp = session.get(url, headers=headers, impersonate="chrome124")
                        if resp.status_code != 200:
                            raise RuntimeError(f"unexpected chatroom status: {resp.status_code}")
                        payload = resp.json()
                        chatroom_id = str(payload.get("id", "")).strip()
                        if chatroom_id:
                            return chatroom_id
                        raise RuntimeError("empty chatroom id")
                    finally:
                        session.close()

                chatroom_id = await asyncio.to_thread(_sync_req)
                return chatroom_id
            except Exception as exc:
                logger.warning("component=viewer_pool event=resolve_chatroom_id_failed error=%s", exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
        raise RuntimeError("cannot resolve chatroom id")

    async def get_channel_ids(self) -> tuple[str, str | None]:
        if self._channel_id:
            return self._channel_id, self._chatroom_id

        channel = str(self.config.get("kick_channel", "")).strip()
        if not channel:
            raise RuntimeError("kick_channel is required")

        channel_id = await self._resolve_channel_id()
        chatroom_id = await self._resolve_chatroom_id(channel)

        if not channel_id:
            raise RuntimeError("empty channel id")
        if not chatroom_id:
            logger.warning("component=viewer_pool event=chatroom_id_missing channel=%s", channel)

        self._channel_id = channel_id
        self._chatroom_id = chatroom_id
        return self._channel_id, self._chatroom_id

    def _cleanup_done_tasks(self) -> None:
        self.tasks = [task for task in self.tasks if not task.done()]

    def _create_viewer_task(self, worker_index: int, channel_id: str, chatroom_id: str) -> None:
        viewer = KickViewer(
            self.config,
            worker_index + 1,
            channel_id=channel_id,
            chatroom_id=chatroom_id,
            on_ws_connection_change=self._on_ws_connection_change,
        )
        task = asyncio.create_task(viewer.run(), name=f"viewer-{worker_index + 1}")
        self.viewers.append(viewer)
        self.tasks.append(task)
        self._started_workers += 1
        self._emit_telemetry()

    async def start_gradually(self, total_count: int, ramp_up_seconds: float = 60) -> None:
        ramp_up_seconds = max(float(ramp_up_seconds), 0.0)
        rate = total_count / ramp_up_seconds if ramp_up_seconds > 0 else float(total_count)
        logger.info(
            "component=viewer_pool event=ramp_up_start total_count=%s duration_sec=%s rate=%s",
            total_count,
            ramp_up_seconds,
            rate,
        )

        channel_id, chatroom_id = await self.get_channel_ids()
        self._cleanup_done_tasks()

        if total_count <= 0:
            return

        if ramp_up_seconds <= 0:
            for i in range(total_count):
                if not self._running:
                    break
                self._create_viewer_task(i, channel_id, chatroom_id)
            return

        step_interval_sec = ramp_up_seconds / total_count
        for i in range(total_count):
            if not self._running:
                break
            self._create_viewer_task(i, channel_id, chatroom_id)
            if i == total_count - 1:
                break
            try:
                await asyncio.sleep(step_interval_sec)
            except asyncio.CancelledError:
                logger.info("component=viewer_pool event=ramp_up_cancelled started=%s", self._started_workers)
                raise

    async def start(self, ramp_up: float | None = None) -> None:
        if self._running:
            return
        self._running = True
        count = int(self.config.get("viewer_count", 1))
        self._target_workers = count
        self._started_workers = 0
        self._active_ws_connections = 0
        self._emit_telemetry()
        if ramp_up is not None:
            await self.start_gradually(count, ramp_up)
        else:
            channel_id, chatroom_id = await self.get_channel_ids()
            self._cleanup_done_tasks()
            for i in range(count):
                if not self._running:
                    break
                self._create_viewer_task(i, channel_id, chatroom_id)
        self._status_task = asyncio.create_task(self._status_loop(), name="viewer-pool-status")
        logger.info("component=viewer_pool event=started count=%s", count)

    async def _status_loop(self) -> None:
        interval = int(self.config.get("viewer_status_interval_sec", 30))
        while self._running:
            self._cleanup_done_tasks()
            alive = sum(1 for task in self.tasks if not task.done())
            logger.info(
                "component=viewer_pool event=status alive=%s total=%s",
                alive,
                len(self.tasks),
            )
            await asyncio.sleep(interval)

    async def wait(self) -> None:
        if not self.tasks:
            return
        results = await asyncio.gather(*self.tasks, return_exceptions=True)
        for task, result in zip(self.tasks, results):
            if isinstance(result, asyncio.CancelledError):
                logger.info("component=viewer_pool event=viewer_cancelled task=%s", task.get_name())
            elif isinstance(result, Exception):
                logger.error(
                    "component=viewer_pool event=viewer_failed task=%s error=%s",
                    task.get_name(),
                    result,
                )

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        await self.graceful_stop()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self._cleanup_done_tasks()
        self.viewers.clear()
        if self._status_task:
            self._status_task.cancel()
            await asyncio.gather(self._status_task, return_exceptions=True)
            self._status_task = None
        logger.info("component=viewer_pool event=stopped")
        self._active_ws_connections = 0
        self._emit_telemetry()

    async def graceful_stop(self) -> None:
        stop_timeout = float(self.config.get("viewer_stop_timeout_sec", 0.5))
        for viewer in self.viewers:
            try:
                await asyncio.wait_for(viewer.stop(), timeout=stop_timeout)
            except TimeoutError:
                logger.warning(
                    "component=viewer_pool event=viewer_stop_timeout viewer_id=%s timeout=%s",
                    viewer.viewer_id,
                    stop_timeout,
                )
            await asyncio.sleep(random.uniform(0.1, 0.5))
