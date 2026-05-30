import asyncio
import logging
import random
from collections.abc import Callable

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
        self._stopped_event = asyncio.Event()

    def _emit_telemetry(self) -> None:
        if self._telemetry_callback:
            self._telemetry_callback(
                started_workers=self._started_workers,
                target_workers=self._target_workers,
                active_ws_connections=self._active_ws_connections,
            )

    def _cleanup_done_tasks(self) -> None:
        self.tasks = [task for task in self.tasks if not task.done()]

    def _create_viewer_task(self, worker_index: int) -> None:
        viewer = KickViewer(self.config, worker_index + 1)
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

        self._cleanup_done_tasks()

        if total_count <= 0:
            return

        if ramp_up_seconds <= 0:
            for i in range(total_count):
                if not self._running:
                    break
                self._create_viewer_task(i)
            return

        step_interval_sec = ramp_up_seconds / total_count
        for i in range(total_count):
            if not self._running:
                break
            self._create_viewer_task(i)
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
        self._stopped_event = asyncio.Event()
        self._running = True
        count = int(self.config.get("viewer_count", 1))
        self._target_workers = count
        self._started_workers = 0
        self._active_ws_connections = 0
        self._emit_telemetry()
        if ramp_up is not None:
            await self.start_gradually(count, ramp_up)
        else:
            self._cleanup_done_tasks()
            for i in range(count):
                if not self._running:
                    break
                self._create_viewer_task(i)
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

    @staticmethod
    def _log_viewer_task_results(
        tasks: list[asyncio.Task],
        results: list[BaseException | object],
    ) -> None:
        for task, result in zip(tasks, results):
            if isinstance(result, asyncio.CancelledError):
                logger.info("component=viewer_pool event=viewer_cancelled task=%s", task.get_name())
            elif isinstance(result, Exception):
                logger.error(
                    "component=viewer_pool event=viewer_failed task=%s error=%s",
                    task.get_name(),
                    result,
                )

    @staticmethod
    def _consume_waiter_result(waiter: asyncio.Future) -> None:
        try:
            waiter.result()
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("component=viewer_pool event=waiter_result_consume_failed")

    async def _wait_for_viewer_tasks(self, tasks: list[asyncio.Task]) -> None:
        gather_future = asyncio.gather(*tasks, return_exceptions=True)
        try:
            results = await asyncio.shield(gather_future)
        except asyncio.CancelledError:
            gather_future.add_done_callback(self._consume_waiter_result)
            raise
        self._log_viewer_task_results(tasks, results)

    async def wait(self) -> None:
        if not self.tasks:
            await self._stopped_event.wait()
            return

        viewer_tasks = list(self.tasks)
        stopped_waiter = asyncio.create_task(
            self._stopped_event.wait(),
            name="viewer-pool-stopped-waiter",
        )
        viewer_tasks_waiter = asyncio.create_task(
            self._wait_for_viewer_tasks(viewer_tasks),
            name="viewer-pool-tasks-waiter",
        )
        waiters = {stopped_waiter, viewer_tasks_waiter}

        try:
            done, pending = await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
            await asyncio.gather(*done)
        finally:
            pending_waiters = [waiter for waiter in waiters if not waiter.done()]
            for waiter in pending_waiters:
                waiter.cancel()
            if pending_waiters:
                await asyncio.gather(*pending_waiters, return_exceptions=True)

    async def stop(self) -> None:
        if not self._running:
            self._stopped_event.set()
            return
        self._running = False
        try:
            try:
                await self.graceful_stop()
            finally:
                tasks = list(self.tasks)
                for task in tasks:
                    task.cancel()
                results = await asyncio.gather(*tasks, return_exceptions=True) if tasks else []
                self._log_viewer_task_results(tasks, results)
                self.tasks.clear()
                self.viewers.clear()
                if self._status_task:
                    self._status_task.cancel()
                    await asyncio.gather(self._status_task, return_exceptions=True)
                    self._status_task = None
                logger.info("component=viewer_pool event=stopped")
                self._active_ws_connections = 0
                self._emit_telemetry()
        finally:
            self._stopped_event.set()

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
