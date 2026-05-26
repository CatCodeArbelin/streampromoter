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
        self._throttle = int(self.config.get("openai_throttle_sec", 15))
        self._last_published_at = 0.0

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
        now = time.time()
        if now - self._last_published_at < self._throttle:
            return

        self._last_published_at = now
        await self.chat_poster.post(text)

    async def stop(self) -> None:
        self._running = False
