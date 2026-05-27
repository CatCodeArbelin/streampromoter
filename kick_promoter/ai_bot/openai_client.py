import asyncio
import contextlib
import json
import logging
import time

import websockets

from kick_promoter.ai_bot.audio_capture import AudioCapture

logger = logging.getLogger(__name__)


class OpenAIClient:
    def __init__(self, config: dict, chat_poster):
        self.config = config
        self.chat_poster = chat_poster
        self.enabled = bool(config.get("openai_enabled", False))
        self._task = None
        self._running = True
        self._throttle = int(self.config.get("message_cooldown_seconds", self.config.get("openai_throttle_sec", 15)))
        self._last_published_at = 0.0
        self._outbox: asyncio.Queue[str | None] = asyncio.Queue()
        self._poster_task: asyncio.Task | None = None
        self._last_enqueued_text: str | None = None

    async def run(self) -> None:
        if not self.enabled:
            logger.info("OpenAI disabled, using fallback phrases")
            await self.chat_poster.fallback_loop()
            return

        realtime_endpoint = self.config.get("openai_realtime_endpoint", "wss://api.openai.com/v1/realtime")
        model = self.config.get("openai_model", "gpt-4o-realtime-preview")
        uri = f"{realtime_endpoint}?model={model}"
        headers = {
            "Authorization": f"Bearer {self.config.get('openai_api_key', '')}",
            "OpenAI-Beta": "realtime=v1",
        }
        audio = AudioCapture(f"https://kick.com/{self.config.get('kick_channel')}")
        await audio.start()
        backoff = 1
        max_backoff = int(self.config.get("openai_reconnect_max_backoff_sec", 30))

        try:
            self._poster_task = asyncio.create_task(self._poster_loop())
            while self._running:
                try:
                    async with websockets.connect(uri, additional_headers=headers, ping_interval=20) as ws:
                        await ws.send(
                            json.dumps(
                                {
                                    "type": "session.update",
                                    "session": {
                                        "voice": self.config.get("openai_voice", "alloy"),
                                        "modalities": ["text"],
                                        "instructions": self.config.get(
                                            "openai_instructions",
                                            "Пиши коротко, остроумно и дружелюбно, с легким юмором.",
                                        ),
                                        "input_audio_transcription": {"enabled": True},
                                    },
                                }
                            )
                        )
                        backoff = 1
                        consumer = asyncio.create_task(self._consume(ws))
                        try:
                            async for chunk in audio.chunks_base64():
                                if not self._running:
                                    break
                                await ws.send(
                                    json.dumps(
                                        {
                                            "type": "input_audio_buffer.append",
                                            "audio": chunk,
                                        }
                                    )
                                )
                        finally:
                            consumer.cancel()
                            with contextlib.suppress(asyncio.CancelledError):
                                await consumer
                except websockets.ConnectionClosed as exc:
                    if not self._running:
                        break
                    logger.warning("Realtime websocket disconnected (%s). Reconnecting in %ss", exc, backoff)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)
                except Exception:
                    if not self._running:
                        break
                    logger.exception("Unexpected realtime client error. Reconnecting in %ss", backoff)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)
        finally:
            await self._shutdown_poster_worker()
            await audio.stop()

    async def _consume(self, ws) -> None:
        pending = {}

        async for message in ws:
            data = json.loads(message)
            msg_type = data.get("type")

            if msg_type == "response.text.delta":
                key = (
                    data.get("response_id"),
                    data.get("output_index", 0),
                    data.get("content_index", 0),
                )
                pending[key] = pending.get(key, "") + data.get("delta", "")
            elif msg_type == "response.text.done":
                key = (
                    data.get("response_id"),
                    data.get("output_index", 0),
                    data.get("content_index", 0),
                )
                text = (pending.pop(key, "") or data.get("text", "")).strip()
                if text:
                    await self._post_final(text)

    async def _post_final(self, text: str) -> None:
        cleaned = text.strip()
        if not cleaned:
            return
        if cleaned == self._last_enqueued_text:
            return
        self._last_enqueued_text = cleaned
        await self._outbox.put(cleaned)

    async def _poster_loop(self) -> None:
        while self._running:
            text = await self._outbox.get()
            if text is None:
                break

            if self.chat_poster.should_skip_cycle():
                logger.info(
                    "component=openai_client event=skip_post_cycle reason=bernoulli viewer_channel=%s",
                    self.config.get("kick_channel", ""),
                )
                continue

            delay = self.chat_poster.compute_next_delay(min_delay=float(self._throttle))
            logger.debug("component=openai_client event=pre_post_delay delay_sec=%.3f", delay)
            await asyncio.sleep(delay)

            try:
                await self.chat_poster.post(text)
                self._last_published_at = time.time()
            except Exception:
                logger.exception("Failed to post final text")

    async def _shutdown_poster_worker(self) -> None:
        if not self._poster_task:
            return

        await self._outbox.put(None)
        try:
            await asyncio.wait_for(self._poster_task, timeout=5)
        except asyncio.TimeoutError:
            logger.warning("Poster worker did not stop in time, cancelling")
            self._poster_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._poster_task
        finally:
            self._poster_task = None

    async def stop(self) -> None:
        self._running = False
        with contextlib.suppress(asyncio.QueueFull):
            self._outbox.put_nowait(None)
