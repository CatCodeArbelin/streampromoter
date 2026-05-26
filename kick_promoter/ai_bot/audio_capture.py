import asyncio
import logging

logger = logging.getLogger(__name__)


class AudioCapture:
    CHUNK_SIZE = 96000

    def __init__(self, stream_url: str):
        self.stream_url = stream_url
        self.streamlink_proc = None
        self.ffmpeg_proc = None

    async def start(self) -> None:
        self.streamlink_proc = await asyncio.create_subprocess_exec(
            "streamlink",
            "--stdout",
            self.stream_url,
            "best",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        self.ffmpeg_proc = await asyncio.create_subprocess_exec(
            "ffmpeg",
            "-i",
            "pipe:0",
            "-f",
            "s16le",
            "-ac",
            "1",
            "-ar",
            "24000",
            "pipe:1",
            stdin=self.streamlink_proc.stdout,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        logger.info("AudioCapture started")

    async def chunks(self):
        if not self.ffmpeg_proc:
            await self.start()
        while True:
            chunk = await self.ffmpeg_proc.stdout.read(self.CHUNK_SIZE)
            if not chunk:
                break
            yield chunk

    async def stop(self) -> None:
        for proc in [self.ffmpeg_proc, self.streamlink_proc]:
            if proc and proc.returncode is None:
                proc.terminate()
                await proc.wait()
