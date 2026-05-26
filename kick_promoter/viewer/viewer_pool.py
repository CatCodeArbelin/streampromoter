import asyncio
import logging
import random

from kick_promoter.viewer.kick_viewer import KickViewer

logger = logging.getLogger(__name__)


class ViewerPool:
    def __init__(self, config: dict):
        self.config = config
        self.tasks: list[asyncio.Task] = []
        self.viewers: list[KickViewer] = []
        self._status_task: asyncio.Task | None = None
        self._running = False

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        count = int(self.config.get("viewer_count", 1))
        for i in range(count):
            viewer = KickViewer(self.config, i + 1)
            task = asyncio.create_task(viewer.run(), name=f"viewer-{i + 1}")
            self.viewers.append(viewer)
            self.tasks.append(task)
        self._status_task = asyncio.create_task(self._status_loop(), name="viewer-pool-status")
        logger.info("component=viewer_pool event=started count=%s", count)

    async def _status_loop(self) -> None:
        interval = int(self.config.get("viewer_status_interval_sec", 30))
        while self._running:
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
        if self._status_task:
            self._status_task.cancel()
            await asyncio.gather(self._status_task, return_exceptions=True)
            self._status_task = None
        logger.info("component=viewer_pool event=stopped")

    async def graceful_stop(self) -> None:
        for viewer in self.viewers:
            await viewer.stop()
            await asyncio.sleep(random.uniform(0.1, 0.5))
