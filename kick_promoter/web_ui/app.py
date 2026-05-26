import asyncio
import json
import logging
import threading
from queue import Queue

from flask import Flask, Response, jsonify, render_template, request

from kick_promoter.main import load_config, run_bot

app = Flask(__name__)
state = {"running": False, "thread": None, "loop": None, "task": None, "logs": Queue()}


class SSELogHandler(logging.Handler):
    def emit(self, record):
        state["logs"].put(self.format(record))


@app.route("/")
def index():
    return render_template("index.html", config=load_config(), running=state["running"])


@app.route("/start", methods=["POST"])
def start():
    if state["running"]:
        return jsonify({"ok": True, "status": "already running"})
    config = request.json or load_config()

    def worker():
        loop = asyncio.new_event_loop()
        state["loop"] = loop
        asyncio.set_event_loop(loop)
        state["task"] = loop.create_task(run_bot(config))
        loop.run_until_complete(state["task"])

    state["thread"] = threading.Thread(target=worker, daemon=True)
    state["thread"].start()
    state["running"] = True
    return jsonify({"ok": True})


@app.route("/stop", methods=["POST"])
def stop():
    if not state["running"]:
        return jsonify({"ok": True, "status": "already stopped"})
    if state["task"]:
        state["loop"].call_soon_threadsafe(state["task"].cancel)
    state["running"] = False
    return jsonify({"ok": True})


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
