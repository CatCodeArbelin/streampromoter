# StreamPromoter

Асинхронный бот для Kick, который:
- поднимает пул «зрителей» (WebSocket-подключения к чату),
- публикует фразы в чат либо из `phrases.txt`, либо через OpenAI Realtime,
- управляется через Web UI (`/` на Flask).

## 1) Назначение проекта и архитектура

### Назначение
`streampromoter` нужен для автоматизации активности в чате Kick: эмуляция стабильного online через несколько подключений и регулярная отправка сообщений.

### Архитектура модулей
- `kick_promoter/main.py` — точка входа, загрузка конфигурации и запуск всех подсистем.
- `kick_promoter/viewer/`:
  - `viewer_pool.py` — жизненный цикл пула зрителей;
  - `kick_viewer.py` — один WebSocket-клиент зрителя, reconnect + ping.
- `kick_promoter/ai_bot/`:
  - `chat_poster.py` — отправка сообщений в Kick API + retry/backoff;
  - `openai_client.py` — OpenAI Realtime-режим;
  - `audio_capture.py` — захват аудио через `streamlink` + `ffmpeg`.
- `kick_promoter/web_ui/` — Flask UI (start/stop/logs).

## 2) Требования

Минимум:
- `Python 3.11+`;
- `ffmpeg` (обязательно для аудиозахвата);
- `streamlink` (источник аудио потока);
- Docker + Docker Compose (для контейнерного запуска).

Проверка локально:

```bash
python --version
ffmpeg -version
streamlink --version
docker --version
docker compose version
```

## 3) Быстрый старт

### Локальный запуск

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m kick_promoter.web_ui.app
```

После запуска Web UI доступен по `http://127.0.0.1:5000` (или по `web_host:web_port` из `config.json`).

### Запуск через Docker Compose

```bash
docker-compose up --build
```

или (новый синтаксис):

```bash
docker compose up --build
```

## 4) Конфигурация: `config.json`

Пример:

```json
{
  "kick_channel": "example_channel",
  "kick_chatroom_id": "",
  "chat_token": "",
  "viewer_count": 5,
  "viewer_ping_interval_sec": 20,
  "chat_ping_interval_sec": 20,
  "openai_enabled": false,
  "openai_api_key": "",
  "openai_model": "gpt-realtime",
  "openai_voice": "alloy",
  "openai_throttle_sec": 15,
  "post_interval_sec": 30,
  "web_host": "0.0.0.0",
  "web_port": 5000
}
```

### Пояснение полей
- `kick_channel` — slug канала Kick.
- `kick_chatroom_id` — id chatroom для POST сообщений.
- `chat_token` — токен авторизации чата Kick (если требуется для вашего аккаунта/эндпоинта).
- `viewer_count` — число параллельных viewer-сессий.
- `viewer_ping_interval_sec` — интервал ping для viewer WebSocket.
- `openai_enabled` — переключатель OpenAI режима (`true/false`).
- `openai_api_key` — API ключ OpenAI.
- `openai_model`, `openai_voice` — параметры realtime-сессии.
- `openai_throttle_sec` — минимальный интервал отправки аудио чанков в OpenAI.
- `post_interval_sec` — интервал отправки fallback-фраз.
- `web_host`, `web_port` — bind Web UI.

## 5) Переменные окружения

Поддерживаются env-переопределения (приоритет выше `config.json`):

- `KICK_CHANNEL`
- `KICK_CHATROOM_ID`
- `CHAT_TOKEN`
- `VIEWER_COUNT`
- `VIEWER_PING_INTERVAL_SEC`
- `POST_INTERVAL_SEC`
- `OPENAI_ENABLED` (`1/true/yes/on` → `true`)
- `OPENAI_API_KEY`
- `OPENAI_MODEL`
- `OPENAI_VOICE`
- `OPENAI_THROTTLE_SEC`
- `WEB_HOST`
- `WEB_PORT`

Пример:

```bash
export KICK_CHANNEL=my_channel
export KICK_CHATROOM_ID=123456
export CHAT_TOKEN='your_kick_token'
export OPENAI_ENABLED=true
export OPENAI_API_KEY='sk-...'
python -m kick_promoter.web_ui.app
```

## 6) Включение / выключение OpenAI-режима

- Выключить: `openai_enabled=false` (или `OPENAI_ENABLED=false`) — бот будет отправлять случайные фразы из `phrases.txt`.
- Включить: `openai_enabled=true` + валидный `openai_api_key` (или `OPENAI_API_KEY`).

## 7) Пример `phrases.txt`

```text
Отличный стрим, продолжаем!
Крутая атмосфера в чате 🔥
Респект стримеру за контент!
Залетайте почаще, тут интересно!
Топовый момент, спасибо за стрим!
```

## 8) Ограничения и нагрузка (rate-limit)

- Kick API может ограничивать частоту сообщений; слишком маленький `post_interval_sec` повышает риск 429/блокировок.
- В коде есть retry с экспоненциальным backoff на отправку сообщений.
- `viewer_count` напрямую увеличивает число постоянных WebSocket-соединений и нагрузку на сеть/CPU.
- В OpenAI режиме `openai_throttle_sec` сдерживает частоту отправки аудио в Realtime API; уменьшение значения повышает токен/трафик/стоимость.

Рекомендации для старта:
- `viewer_count`: 2–5
- `post_interval_sec`: 20–45 сек
- `openai_throttle_sec`: 10–20 сек

## 9) Troubleshooting

### 9.1 WebSocket reconnect ошибки
Симптомы:
- в логах `ws reconnect after error`.

Проверьте:
- доступ к `wss://ws-us2.pusher.com`;
- стабильность сети/VPN/прокси;
- не слишком ли агрессивные интервалы ping и количество viewers.

Что делать:
- снизить `viewer_count`;
- увеличить `viewer_ping_interval_sec`;
- перезапустить сервис и проверить сетевые ограничения у провайдера.

### 9.2 Недоступность `streamlink` / `ffmpeg`
Симптомы:
- OpenAI-режим стартует, но нет аудио/появляются ошибки subprocess.

Проверьте:
```bash
streamlink --version
ffmpeg -version
```

Что делать:
- установить обе утилиты в PATH;
- для Docker убедиться, что образ собирается без ошибок и содержит `ffmpeg`.

### 9.3 Проблемы авторизации Kick / OpenAI
Симптомы:
- 401/403 при отправке в Kick;
- ошибки аутентификации при подключении к OpenAI Realtime.

Проверьте:
- `kick_chatroom_id` и `chat_token`/`CHAT_TOKEN`;
- валидность `OPENAI_API_KEY`;
- что `openai_enabled=true` только при реально доступном ключе.

Что делать:
- обновить токены/ключи;
- сверить переменные окружения в текущем shell/контейнере (`printenv | rg 'OPENAI|KICK|CHAT_TOKEN'`).
