"""
app.py - Anypoint MQ stats web app

Run:
    pip install -r requirements.txt
    python app.py
    # open http://localhost:5000

Flow:
  1. GET  /                fill in org/env/region/period/credentials
  2. POST /fetch           kicks off a background fetch, returns a job id
  3. GET  /fetch/<id>       poll job status (stage, detail, done, error)
  4. GET  /dashboard        D3 dashboard, reads the latest completed fetch
  5. GET  /api/stats        the current mq_stats.json as JSON (used by the dashboard)

State is in-memory only (one process, fine for a single user running this
locally). mq_stats.json is also written to disk next to app.py so it
survives a restart and can be reused by other tools (e.g. a notebook).
"""

import json
import os
import threading
import uuid
from datetime import datetime, timezone

from flask import Flask, jsonify, render_template, request, abort

from mq_stats_lib import run_fetch, PERIODS, FetchError

app = Flask(__name__)

DATA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mq_stats.json")

# job_id -> {"stage", "detail", "done", "error", "doc"}
JOBS = {}
JOBS_LOCK = threading.Lock()

# the most recent successful fetch result, kept in memory and mirrored to disk
LATEST = {"doc": None}
if os.path.exists(DATA_FILE):
    try:
        with open(DATA_FILE) as f:
            LATEST["doc"] = json.load(f)
    except Exception:
        pass


def _set_job(job_id, **kw):
    with JOBS_LOCK:
        JOBS[job_id].update(kw)


def _run_job(job_id, params):
    def on_progress(stage, detail=""):
        _set_job(job_id, stage=stage, detail=detail)

    try:
        doc = run_fetch(on_progress=on_progress, **params)
        with open(DATA_FILE, "w") as f:
            json.dump(doc, f, indent=2)
        LATEST["doc"] = doc
        _set_job(job_id, stage="done", detail="Complete", done=True, error=None)
    except FetchError as e:
        _set_job(job_id, stage="error", detail=str(e), done=True, error=str(e))
    except Exception as e:  # noqa: BLE001 - surface anything unexpected to the UI
        _set_job(job_id, stage="error", detail=f"Unexpected error: {e}",
                 done=True, error=str(e))


@app.route("/")
def index():
    return render_template(
        "index.html",
        periods=list(PERIODS.keys()),
        has_data=LATEST["doc"] is not None,
        env_client_id=os.environ.get("ANYPOINT_CLIENT_ID", ""),
    )


@app.route("/fetch", methods=["POST"])
def start_fetch():
    form = request.form
    client_id = form.get("client_id") or os.environ.get("ANYPOINT_CLIENT_ID", "")
    client_secret = form.get("client_secret") or os.environ.get("ANYPOINT_CLIENT_SECRET", "")

    if not client_id or not client_secret:
        return jsonify({"error": "Client ID and secret are required (fill the form "
                                  "or set ANYPOINT_CLIENT_ID / ANYPOINT_CLIENT_SECRET)."}), 400

    params = dict(
        org_id=form.get("org_id", "").strip(),
        env_id=form.get("env_id", "").strip(),
        region=form.get("region", "").strip(),
        period=form.get("period", "hour"),
        client_id=client_id,
        client_secret=client_secret,
        start=form.get("start") or None,
        end=form.get("end") or None,
        include_exchanges=form.get("include_exchanges") == "on",
    )

    job_id = uuid.uuid4().hex
    with JOBS_LOCK:
        JOBS[job_id] = {"stage": "queued", "detail": "", "done": False, "error": None}

    thread = threading.Thread(target=_run_job, args=(job_id, params), daemon=True)
    thread.start()
    return jsonify({"job_id": job_id})


@app.route("/fetch/<job_id>")
def fetch_status(job_id):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
    if job is None:
        abort(404)
    return jsonify({k: v for k, v in job.items()})


@app.route("/dashboard")
def dashboard():
    if LATEST["doc"] is None:
        return render_template("index.html", periods=list(PERIODS.keys()),
                               has_data=False, no_data_notice=True,
                               env_client_id=os.environ.get("ANYPOINT_CLIENT_ID", ""))
    return render_template("dashboard.html")


@app.route("/api/stats")
def api_stats():
    if LATEST["doc"] is None:
        abort(404)
    return jsonify(LATEST["doc"])


@app.route("/api/sample", methods=["POST"])
def load_sample():
    """Load bundled sample data so the dashboard can be tried without credentials."""
    sample_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mq_stats.sample.json")
    if not os.path.exists(sample_path):
        abort(404)
    with open(sample_path) as f:
        doc = json.load(f)
    LATEST["doc"] = doc
    with open(DATA_FILE, "w") as f:
        json.dump(doc, f, indent=2)
    return jsonify({"ok": True})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True, use_reloader=False)
