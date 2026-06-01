import asyncio
import json
import logging
import threading
import traceback
from queue import Queue

from flask import Flask, Response, jsonify, render_template, request
from waitress import serve as waitress_serve

from kick_promoter.main import Runner, load_config

app = Flask(__name__)
logger = logging.getLogger(__name__)
STOP_JOIN_TIMEOUT_SEC = 10


_UNSET = object()


class AppState:
    def __init__(self):
        self.lock = threading.Lock()
        self.logs = Queue()
        self.events = Queue()
        self.running = False
        self.status = "stopped"
        self.error = None
        self.thread = None
        self.loop = None
        self.task = None
        self.runtime = self._default_runtime()

    @staticmethod
    def _default_runtime() -> dict:
        return {
            "progress": 0,
            "started_workers": 0,
            "target_workers": 0,
            "active_ws_connections": 0,
            "ai_last_messages": [],
        }

    def get_status_snapshot(self) -> dict:
        with self.lock:
            return {
                "running": self.running,
                "status": self.status,
                "error": self.error,
                "runtime": dict(self.runtime),
            }

    def set_lifecycle(self, *, running=_UNSET, status=_UNSET, error=_UNSET, thread=_UNSET, loop=_UNSET, task=_UNSET, reset_runtime=False):
        with self.lock:
            if running is not _UNSET:
                self.running = running
            if status is not _UNSET:
                self.status = status
            if error is not _UNSET:
                self.error = error
            if thread is not _UNSET:
                self.thread = thread
            if loop is not _UNSET:
                self.loop = loop
            if task is not _UNSET:
                self.task = task
            if reset_runtime:
                self.runtime = self._default_runtime()

    def update_runtime(self, **payload):
        with self.lock:
            for field in ("started_workers", "target_workers", "active_ws_connections"):
                if field in payload:
                    self.runtime[field] = int(payload[field])
            target = self.runtime.get("target_workers", 0)
            started = self.runtime.get("started_workers", 0)
            self.runtime["progress"] = int((started / target) * 100) if target > 0 else 0
            ai_message = payload.get("ai_message")
            if ai_message:
                self.runtime["ai_last_messages"] = (self.runtime["ai_last_messages"] + [str(ai_message)])[-5:]


    def get_worker_handles(self):
        with self.lock:
            return self.thread, self.loop, self.task

    def append_log(self, message: str):
        self.logs.put(message)

    def append_event(self, event: str, **payload):
        self.events.put({"event": event, **payload})


state = AppState()


class ServiceManager:
    def __init__(self):
        self.runner: Runner | None = None

    def status(self) -> dict:
        if self.runner:
            return self.runner.status()
        return {"status": "stopped", "started": False, "stopping": False}


service_manager = ServiceManager()


def publish_event(event: str, **payload):
    if event == "telemetry":
        state.update_runtime(**payload)
    state.append_event(event, **payload)


class SSELogHandler(logging.Handler):
    def emit(self, record):
        state.append_log(self.format(record))


@app.route("/")
def index():
    return render_template("index.html", config=load_config(), state=state.get_status_snapshot())


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
    if state.get_status_snapshot()["running"]:
        return jsonify({"ok": True, "status": "already_running"})

    config, errors = _parse_runtime_request(load_config())
    if errors:
        return jsonify({"ok": False, "errors": errors}), 400

    state.set_lifecycle(status="starting", error=None, reset_runtime=True)
    publish_event("lifecycle", phase="start", progress=10, status=state.get_status_snapshot()["status"], message="Starting workers")

    def worker():
        loop = asyncio.new_event_loop()
        state.set_lifecycle(loop=loop)
        asyncio.set_event_loop(loop)
        service_manager.runner = Runner(config, telemetry_callback=lambda **payload: publish_event("telemetry", **payload))
        publish_event("lifecycle", phase="start", progress=40, status="starting", message="Runtime initialized")
        task = loop.create_task(service_manager.runner.start())
        state.set_lifecycle(task=task)
        try:
            logger.info("component=web_ui event=worker_loop_run_until_complete")
            loop.run_until_complete(task)
            task_exc = task.exception() if task.done() and not task.cancelled() else None
            if task_exc is not None:
                task_tb_text = "".join(traceback.format_exception(type(task_exc), task_exc, task_exc.__traceback__))
                err_text = str(task_exc) or task_exc.__class__.__name__
                full_error = f"{err_text}\n\n{task_tb_text}"
                state.set_lifecycle(status="error", error=full_error)
                publish_event("lifecycle", phase="error", progress=100, status=state.get_status_snapshot()["status"], message=full_error)
            elif state.get_status_snapshot()["status"] != "error":
                state.set_lifecycle(status="stopped")
                publish_event("lifecycle", phase="stop", progress=100, status=state.get_status_snapshot()["status"], message="Stopped")
        except asyncio.CancelledError:
            logger.info("component=web_ui event=worker_task_cancelled")
            state.set_lifecycle(status="stopped")
            publish_event("lifecycle", phase="stop", progress=100, status=state.get_status_snapshot()["status"], message="Stopped")
        except Exception as exc:
            tb_text = traceback.format_exc()
            err_text = str(exc) or exc.__class__.__name__
            full_error = f"{err_text}\n\n{tb_text}"
            logger.exception("Runner failed with traceback")
            state.set_lifecycle(status="error", error=full_error)
            publish_event("lifecycle", phase="error", progress=100, status=state.get_status_snapshot()["status"], message=full_error)
        finally:
            pending = [pending_task for pending_task in asyncio.all_tasks(loop) if not pending_task.done()]
            for pending_task in pending:
                pending_task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.close()
            state.set_lifecycle(running=False, loop=None, task=None, thread=None)

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    state.set_lifecycle(thread=thread, running=True)
    return jsonify({"ok": True, "status": state.get_status_snapshot()["status"]})


@app.route("/stop", methods=["POST"])
def stop():
    if not state.get_status_snapshot()["running"]:
        state.set_lifecycle(status="stopped")
        return jsonify({"ok": True, "status": "already_stopped"})
    state.set_lifecycle(status="stopping")
    publish_event("lifecycle", phase="stop", progress=10, status=state.get_status_snapshot()["status"], message="Stopping workers")
    thread, loop, _task = state.get_worker_handles()
    runner = service_manager.runner

    if loop and runner:
        asyncio.run_coroutine_threadsafe(runner.stop(), loop)
        publish_event("lifecycle", phase="stop", progress=50, status=state.get_status_snapshot()["status"], message="Stop requested")

    # Runner.start() waits on Runner.stop()'s signal; avoid cancelling the root task
    # immediately so its cleanup can complete inside loop.run_until_complete(task).

    if thread:
        thread.join(timeout=STOP_JOIN_TIMEOUT_SEC)
        if thread.is_alive():
            state.set_lifecycle(status="stopping")
            return jsonify({"ok": False, "status": "stopping", "error": "stop timeout"}), 504

    service_manager.runner = None
    state.set_lifecycle(loop=None, task=None, thread=None, running=False, status="stopped")
    publish_event("lifecycle", phase="stop", progress=100, status=state.get_status_snapshot()["status"], message="Stopped")
    return jsonify({"ok": True, "status": state.get_status_snapshot()["status"]})


@app.route("/status")
def status():
    snapshot = state.get_status_snapshot()
    return jsonify(
        {
            "running": snapshot["running"],
            "status": snapshot["status"],
            "error": snapshot["error"],
            "service": service_manager.status(),
            "runtime": snapshot["runtime"],
        }
    )


@app.route("/logs")
def logs():
    def stream():
        while True:
            line = state.logs.get()
            yield f"data: {line}\n\n"

    return Response(stream(), mimetype="text/event-stream")


@app.route("/events")
def events():
    def stream():
        while True:
            event = state.events.get()
            yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"

    return Response(stream(), mimetype="text/event-stream")


if __name__ == "__main__":
    handler = SSELogHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
    logging.getLogger().addHandler(handler)
    cfg = load_config()
    host = cfg.get("web_host", "0.0.0.0")
    port = int(cfg.get("web_port", 5000))
    use_dev_server = bool(cfg.get("web_use_dev_server", False))
    if use_dev_server:
        app.run(host=host, port=port)
    else:
        waitress_serve(app, host=host, port=port)
