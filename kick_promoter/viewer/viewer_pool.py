import asyncio
import logging

from kick_promoter.viewer.kick_viewer import KickViewer

logger = logging.getLogger(__name__)


class ViewerPool:
    def __init__(self, config: dict):
        self.config = config
        self.tasks: list[asyncio.Task] = []
        self.viewers: list[KickViewer] = []

    async def start(self) -> None:
        count = int(self.config.get("viewer_count", 1))
        for i in range(count):
            viewer = KickViewer(self.config, i + 1)
            task = asyncio.create_task(viewer.run(), name=f"viewer-{i + 1}")
            self.viewers.append(viewer)
            self.tasks.append(task)
        logger.info("Started %s viewers", count)

    async def wait(self) -> None:
        await asyncio.gather(*self.tasks)

    async def stop(self) -> None:
        for viewer in self.viewers:
            await viewer.stop()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        logger.info("ViewerPool stopped gracefully")
