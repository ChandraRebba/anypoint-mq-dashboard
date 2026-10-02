"""
mq_stats_lib.py

Core Anypoint MQ Stats API client, shared by app.py (the web app) and usable
standalone from a script or notebook. No argparse, no globals beyond what's
passed in, so it is safe to import and call repeatedly from a long-running
web process.
"""

import json
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

BASE = "https://anypoint.mulesoft.com"

# Granularity -> (period in seconds, default lookback window)
PERIODS = {
    "15m":    (900,     timedelta(hours=24)),
    "hour":   (3600,    timedelta(days=7)),
    "daily":  (86400,   timedelta(days=30)),
    "weekly": (604800,  timedelta(weeks=12)),
}

# Raw metric names the Stats API returns, mapped to short keys.
RAW_METRICS = {
    "messagesSent":     "sent",
    "messagesReceived": "received",
    "messagesAcked":    "acked",
    "messagesVisible":  "visible",
    "messages":         "depth",      # historical endpoint: messages in queue
    "messagesInflight": "inflight",
    "inflightMessages": "inflight",
    "messagesNacked":   "nacked",
}


class FetchError(Exception):
    pass


def http_json(method, url, headers=None, body=None, retries=3, allow_404=False):
    """Small HTTP helper with retry on 429/5xx. allow_404 returns None on 404."""
    headers = dict(headers or {})
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"

    for attempt in range(1, retries + 1):
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 404 and allow_404:
                return None
            if e.code in (429, 500, 502, 503, 504) and attempt < retries:
                wait = 2 ** attempt
                print(f"  HTTP {e.code}, retrying in {wait}s ...", file=sys.stderr)
                time.sleep(wait)
                continue
            detail = e.read().decode("utf-8", "replace")[:500]
            raise FetchError(f"HTTP {e.code} for {url}\n{detail}")
        except urllib.error.URLError as e:
            raise FetchError(f"Network error for {url}: {e.reason}")


def get_token(client_id, client_secret):
    url = f"{BASE}/accounts/api/v2/oauth2/token"
    payload = {"grant_type": "client_credentials",
               "client_id": client_id, "client_secret": client_secret}
    resp = http_json("POST", url, body=payload)
    token = resp.get("access_token")
    if not token:
        raise FetchError(f"No access_token in response: {resp}")
    return token


def list_destinations(token, org, env, region):
    url = (f"{BASE}/mq/admin/api/v1/organizations/{org}"
           f"/environments/{env}/regions/{region}/destinations?limit=500")
    resp = http_json("GET", url, headers={"Authorization": f"Bearer {token}"})
    items = resp if isinstance(resp, list) else resp.get("destinations", [])
    dests = []
    for d in items:
        dtype = "exchange" if d.get("exchangeId") or d.get("type") == "exchange" else "queue"
        name = d.get("queueId") or d.get("exchangeId") or d.get("destinationId")
        if name:
            dests.append({"name": name, "type": dtype, "fifo": bool(d.get("fifo"))})
    return dests


def iso_utc(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def rfc1123(dt):
    """Stats API date format, e.g. 'Thu, 26 Jul 2024 00:00:00 GMT'."""
    return dt.astimezone(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")


def fetch_stats(token, org, env, region, kind, ids, start, end, period_s,
                 on_progress=None):
    """
    kind: 'queues' or 'exchanges'.
    Historical time series only exist on the per-destination endpoints
    /{kind}/{id}?startDate=&endDate=&period=. The batch endpoint
    /{kind}?destinationIds= returns a live snapshot (current depth and
    inflight count) and ignores the date parameters, which produces empty
    series after normalization. So this fetches one destination at a time.
    Stats endpoints allow roughly 200 requests per minute, so a short pause
    sits between calls. on_progress(n, total) is called after each fetch.
    """
    auth = {"Authorization": f"Bearer {token}"}
    window = urllib.parse.urlencode({
        "startDate": rfc1123(start),
        "endDate": rfc1123(end),
        "period": str(period_s),
    })
    base = (f"{BASE}/mq/stats/api/v1/organizations/{org}"
            f"/environments/{env}/regions/{region}/{kind}")

    results = []
    total = len(ids)
    for n, dest in enumerate(ids, 1):
        url = f"{base}/{urllib.parse.quote(dest, safe='')}?{window}"
        resp = http_json("GET", url, headers=auth, allow_404=True)
        if resp is None:
            print(f"  Warn: no stats endpoint for {kind[:-1]} '{dest}', skipped.")
        else:
            entry = resp[0] if isinstance(resp, list) and resp else resp
            if isinstance(entry, dict):
                entry.setdefault("destination", dest)
                results.append(entry)
        if on_progress:
            on_progress(n, total)
        time.sleep(0.35)   # stay under the Stats API rate limit
    return results


def normalize(entry):
    """
    Turn one Stats API destination entry into:
      {"name": ..., "series": [{"ts": epoch_ms, sent, received, acked,
                                 visible, inflight, success, failure,
                                 reprocessing}, ...]}
    """
    name = entry.get("destination") or entry.get("destinationId") or entry.get("queueId")
    buckets = {}

    for raw_key, short in RAW_METRICS.items():
        for point in entry.get(raw_key) or []:
            ts = point.get("timestamp") or point.get("date")
            if ts is None:
                continue
            if isinstance(ts, str):
                ts = int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1000)
            b = buckets.setdefault(int(ts), {})
            b[short] = b.get(short, 0) + (point.get("value") or 0)

    series = []
    for ts in sorted(buckets):
        b = buckets[ts]
        received = b.get("received", 0)
        acked    = b.get("acked", 0)
        nacked   = b.get("nacked", 0)
        failure = nacked if nacked else max(received - acked, 0)
        reprocessing = b.get("inflight", 0)
        series.append({
            "ts": ts,
            "sent": b.get("sent", 0),
            "received": received,
            "acked": acked,
            "visible": b.get("visible", b.get("depth", 0)),
            "inflight": b.get("inflight", 0),
            "success": acked,
            "failure": failure,
            "reprocessing": reprocessing,
        })
    return {"name": name, "series": series}


def run_fetch(*, org_id, env_id, region, period, client_id, client_secret,
              start=None, end=None, include_exchanges=False,
              on_progress=None):
    """
    Full pipeline: auth -> list destinations -> fetch stats -> normalize ->
    build the doc dict that the dashboard expects. Returns the doc dict.
    on_progress(stage: str, detail: str) is called for status updates.
    """
    def progress(stage, detail=""):
        if on_progress:
            on_progress(stage, detail)

    if period not in PERIODS:
        raise FetchError(f"Unknown period '{period}', must be one of {list(PERIODS)}")

    for label, val in (("org_id", org_id), ("env_id", env_id), ("region", region)):
        if not val or not str(val).strip():
            raise FetchError(f"{label} is blank. A blank id produces a malformed "
                              f"URL and the API answers with a 'No endpoint' error.")

    period_s, default_window = PERIODS[period]
    end_dt = (datetime.fromisoformat(end.replace("Z", "+00:00"))
              if end else datetime.now(timezone.utc))
    start_dt = (datetime.fromisoformat(start.replace("Z", "+00:00"))
                if start else end_dt - default_window)

    progress("auth", "Requesting access token ...")
    token = get_token(client_id, client_secret)

    progress("admin", "Listing destinations ...")
    dests = list_destinations(token, org_id, env_id, region)
    queues = [d["name"] for d in dests if d["type"] == "queue"]
    exchanges = [d["name"] for d in dests if d["type"] == "exchange"]
    progress("admin", f"{len(queues)} queues, {len(exchanges)} exchanges found")

    raw = []
    if queues:
        progress("stats", f"Fetching stats for {len(queues)} queues ...")
        raw += [("queue", e) for e in fetch_stats(
            token, org_id, env_id, region, "queues", queues, start_dt, end_dt,
            period_s,
            on_progress=lambda n, t: progress("stats", f"queues {n}/{t}"))]
    if include_exchanges and exchanges:
        progress("stats", f"Fetching stats for {len(exchanges)} exchanges ...")
        raw += [("exchange", e) for e in fetch_stats(
            token, org_id, env_id, region, "exchanges", exchanges, start_dt, end_dt,
            period_s,
            on_progress=lambda n, t: progress("stats", f"exchanges {n}/{t}"))]

    out_queues = []
    for dtype, entry in raw:
        n = normalize(entry)
        n["type"] = dtype
        n["dlq"] = "dlq" in (n["name"] or "").lower()
        out_queues.append(n)

    totals = {}
    for q in out_queues:
        for p in q["series"]:
            t = totals.setdefault(p["ts"], {"ts": p["ts"], "sent": 0, "received": 0,
                                            "success": 0, "failure": 0,
                                            "reprocessing": 0, "visible": 0})
            for k in ("sent", "received", "success", "failure", "reprocessing", "visible"):
                t[k] += p[k]

    doc = {
        "generatedAt": iso_utc(datetime.now(timezone.utc)),
        "org": org_id,
        "env": env_id,
        "region": region,
        "period": period,
        "periodSeconds": period_s,
        "startDate": iso_utc(start_dt),
        "endDate": iso_utc(end_dt),
        "queues": out_queues,
        "totals": [totals[k] for k in sorted(totals)],
    }
    progress("done", f"{len(out_queues)} destinations, {len(doc['totals'])} time buckets")
    return doc
