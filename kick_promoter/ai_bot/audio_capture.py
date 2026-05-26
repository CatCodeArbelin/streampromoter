import asyncio
import base64
import logging

logger = logging.getLogger(__name__)


class AudioCapture:
    SAMPLE_RATE = 24000
    BYTES_PER_SAMPLE = 2
    CHANNELS = 1
    CHUNK_SECONDS = 2
    CHUNK_SIZE = SAMPLE_RATE * BYTES_PER_SAMPLE * CHANNELS * CHUNK_SECONDS

    def __init__(self, stream_url: str):
        self.stream_url = stream_url
        self.streamlink_proc = None
        self.ffmpeg_proc = None
        self._stopped = False

    async def _resolve_stream_url(self) -> str:
        proc = await asyncio.create_subprocess_exec(
            "streamlink",
            self.stream_url,
            "worst",
            "--stream-url",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()

        if proc.returncode != 0:
            err = stderr.decode("utf-8", errors="ignore").strip()
            raise RuntimeError(f"streamlink failed with code {proc.returncode}: {err}")

        resolved_stream_url = stdout.decode("utf-8", errors="ignore").strip()
        if not resolved_stream_url:
            raise RuntimeError("streamlink returned an empty stream URL")
        return resolved_stream_url

    async def _start_ffmpeg(self, resolved_stream_url: str) -> None:
        self.ffmpeg_proc = await asyncio.create_subprocess_exec(
            "ffmpeg",
            "-i",
            resolved_stream_url,
            "-ac",
            "1",
            "-ar",
            "24000",
            "-f",
            "s16le",
            "-acodec",
            "pcm_s16le",
            "pipe:1",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

    async def _terminate_process(self, proc: asyncio.subprocess.Process | None, name: str) -> None:
        if not proc or proc.returncode is not None:
            return
        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            logger.warning("%s did not terminate gracefully; killing", name)
            proc.kill()
            await proc.wait()

    async def start(self) -> None:
        resolved_stream_url = await self._resolve_stream_url()
        await self._start_ffmpeg(resolved_stream_url)
        logger.info("AudioCapture started")

    async def chunks_base64(self):
        backoff_seconds = 1
        while not self._stopped:
            try:
                if not self.ffmpeg_proc or self.ffmpeg_proc.returncode is not None:
                    await self.start()

                chunk = await self.ffmpeg_proc.stdout.read(self.CHUNK_SIZE)
                if not chunk:
                    if self._stopped:
                        break
                    logger.warning("Unexpected EOF from ffmpeg; restarting capture")
                    raise RuntimeError("unexpected EOF from ffmpeg")

                backoff_seconds = 1
                yield base64.b64encode(chunk).decode("utf-8")
            except Exception as exc:
                if self._stopped:
                    break
                logger.warning(
                    "Audio capture failed (%s). Restarting in %ss",
                    exc,
                    backoff_seconds,
                )
                await self.stop()
                await asyncio.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2, 30)

    async def chunks(self):
        async for chunk_b64 in self.chunks_base64():
            yield base64.b64decode(chunk_b64)

    async def stop(self) -> None:
        self._stopped = True
        await self._terminate_process(self.ffmpeg_proc, "ffmpeg")
        await self._terminate_process(self.streamlink_proc, "streamlink")
        self.ffmpeg_proc = None
        self.streamlink_proc = None
