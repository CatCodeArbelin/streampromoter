import asyncio
import json
import logging
import threading
from queue import Queue

from flask import Flask, Response, jsonify, render_template, request

from kick_promoter.main import Runner, load_config

app = Flask(__name__)
state = {
    "running": False,
    "status": "stopped",
    "error": None,
    "thread": None,
    "loop": None,
    "task": None,
    "logs": Queue(),
}
STOP_JOIN_TIMEOUT_SEC = 10


class ServiceManager:
    def __init__(self):
        self.runner: Runner | None = None

    def status(self) -> dict:
        if self.runner:
            return self.runner.status()
        return {"status": "stopped", "started": False, "stopping": False}


service_manager = ServiceManager()


class SSELogHandler(logging.Handler):
    def emit(self, record):
        state["logs"].put(self.format(record))


@app.route("/")
def index():
    return render_template("index.html", config=load_config(), state=state)


def _parse_runtime_request(base_config: dict) -> tuple[dict, list[str]]:
    payload = request.get_json(silent=True) if request.is_json else request.form
    payload = payload or {}

    merged = dict(base_config)
    errors = []

    channel_name = str(payload.get("channel_name", "")).strip()
    if not channel_name:
        errors.append("channel_name is required")
    else:
        merged["kick_channel"] = channel_name

    viewers_raw = str(payload.get("viewers_count", "")).strip()
    try:
        viewers_count = int(viewers_raw)
        if viewers_count < 1:
            raise ValueError
        merged["viewer_count"] = viewers_count
    except ValueError:
        errors.append("viewers_count must be a positive integer")

    chat_token = str(payload.get("chat_token", "")).strip()
    if chat_token:
        merged["chat_token"] = chat_token

    openai_api_key = str(payload.get("openai_api_key", "")).strip()
    if openai_api_key:
        merged["openai_api_key"] = openai_api_key

    enable_raw = str(payload.get("enable_ai_bot", "")).strip().lower()
    merged["openai_enabled"] = enable_raw in {"1", "true", "yes", "on"}

    phrases_text = str(payload.get("phrases_text", "")).strip()
    phrases_file = request.files.get("phrases_file")
    if phrases_file and phrases_file.filename:
        phrases_text = phrases_file.read().decode("utf-8", errors="ignore").strip()

    if phrases_text:
        phrases = [line.strip() for line in phrases_text.splitlines() if line.strip()]
        if phrases:
            merged["runtime_phrases"] = phrases

    return merged, errors


@app.route("/start", methods=["POST"])
def start():
    if state["running"]:
        return jsonify({"ok": True, "status": "already_running"})

    config, errors = _parse_runtime_request(load_config())
    if errors:
        return jsonify({"ok": False, "errors": errors}), 400

    state["status"] = "running"
    state["error"] = None

    def worker():
        loop = asyncio.new_event_loop()
        state["loop"] = loop
        asyncio.set_event_loop(loop)
        service_manager.runner = Runner(config)
        state["task"] = loop.create_task(service_manager.runner.start())
        try:
            loop.run_until_complete(state["task"])
            if state["status"] != "error":
                state["status"] = "stopped"
        except Exception as exc:
            state["status"] = "error"
            state["error"] = str(exc)
            logging.exception("Runner failed")
        finally:
            state["running"] = False

    state["thread"] = threading.Thread(target=worker, daemon=True)
    state["thread"].start()
    state["running"] = True
    return jsonify({"ok": True, "status": state["status"]})


@app.route("/stop", methods=["POST"])
def stop():
    if not state["running"]:
        state["status"] = "stopped"
        return jsonify({"ok": True, "status": "already_stopped"})
    loop = state.get("loop")
    runner = service_manager.runner

    if loop and runner:
        asyncio.run_coroutine_threadsafe(runner.stop(), loop)

    if state["task"] and loop:
        loop.call_soon_threadsafe(state["task"].cancel)

    thread = state.get("thread")
    if thread:
        thread.join(timeout=STOP_JOIN_TIMEOUT_SEC)
        if thread.is_alive():
            state["status"] = "stopping"
            return jsonify({"ok": False, "status": "stopping", "error": "stop timeout"}), 504

    service_manager.runner = None
    state["loop"] = None
    state["task"] = None
    state["thread"] = None
    state["running"] = False
    state["status"] = "stopped"
    return jsonify({"ok": True, "status": state["status"]})


@app.route("/status")
def status():
    return jsonify(
        {
            "running": state["running"],
            "status": state["status"],
            "error": state["error"],
            "service": service_manager.status(),
        }
    )


@app.route("/logs")
def logs():
    def stream():
        while True:
            line = state["logs"].get()
            yield f"data: {line}\n\n"

    return Response(stream(), mimetype="text/event-stream")


if __name__ == "__main__":
    handler = SSELogHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
    logging.getLogger().addHandler(handler)
    cfg = load_config()
    app.run(host=cfg.get("web_host", "0.0.0.0"), port=int(cfg.get("web_port", 5000)))
