import asyncio
import base64
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

    async def run(self) -> None:
        if not self.enabled:
            logger.info("OpenAI disabled, using fallback phrases")
            await self.chat_poster.fallback_loop()
            return

        uri = "wss://api.openai.com/v1/realtime?model=" + self.config.get("openai_model", "gpt-realtime")
        headers = {
            "Authorization": f"Bearer {self.config.get('openai_api_key', '')}",
            "OpenAI-Beta": "realtime=v1",
        }
        audio = AudioCapture(f"https://kick.com/{self.config.get('kick_channel')}")
        await audio.start()
        throttle = int(self.config.get("openai_throttle_sec", 15))
        last_sent = 0.0

        async with websockets.connect(uri, additional_headers=headers, ping_interval=20) as ws:
            await ws.send(
                json.dumps(
                    {
                        "type": "session.update",
                        "session": {
                            "voice": self.config.get("openai_voice", "alloy"),
                            "modalities": ["text"],
                        },
                    }
                )
            )
            consumer = asyncio.create_task(self._consume(ws))
            try:
                async for chunk in audio.chunks():
                    now = time.time()
                    if now - last_sent < throttle:
                        continue
                    last_sent = now
                    await ws.send(
                        json.dumps(
                            {
                                "type": "input_audio_buffer.append",
                                "audio": base64.b64encode(chunk).decode("utf-8"),
                            }
                        )
                    )
            finally:
                consumer.cancel()
                await audio.stop()

    async def _consume(self, ws) -> None:
        async for message in ws:
            data = json.loads(message)
            if data.get("type") == "response.text.delta":
                text = data.get("delta", "").strip()
                if text:
                    await self.chat_poster.post(text)

    async def stop(self) -> None:
        self._running = False
