"""
probe.py - BLOG TEST HARNESS: availability probe for a Neptune writer.

Sends `RETURN 1` (openCypher) every --interval seconds (default 5) and writes one JSON
line per probe:  {"t": "<UTC ISO time the probe was sent>", "latency_ms": <float>,
"error": null | "<error code>" | "timeout"}

It runs until stopped (Ctrl-C or SIGTERM) and then prints a summary: probe count,
errors by code, p50/p99 latency of the successful probes (nearest-rank), and the
longest gap between consecutive successful responses. A probe that takes longer than
the interval skips the ticks it missed instead of bursting. The client does not retry,
so every timeout counts once.

  python scripts/probe.py --endpoint <writer> --region us-east-1 \
      --log results/probe-QUADS.log --summary-out results/probe-QUADS.summary.json

Summarize an existing log (for example after the process was killed):
  python scripts/probe.py --summarize-only results/probe-QUADS.log
"""

import argparse
import datetime as dt
import json
import math
import signal
import sys
import threading
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, ConnectTimeoutError, ReadTimeoutError

TS_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def classify(err):
    """Error code for a failed probe: 'timeout' for client timeouts, else the service
    error code, else the exception class name."""
    if isinstance(err, (ReadTimeoutError, ConnectTimeoutError)):
        return "timeout"
    if isinstance(err, ClientError):
        return err.response.get("Error", {}).get("Code") or "ClientError"
    return type(err).__name__


def nearest_rank(values, p):
    """Nearest-rank percentile of a list (p in 0-100); None for an empty list."""
    if not values:
        return None
    xs = sorted(values)
    return xs[max(0, math.ceil(p / 100 * len(xs)) - 1)]


def _fmt(epoch):
    return dt.datetime.fromtimestamp(epoch, dt.timezone.utc).strftime(TS_FORMAT)


def _parse(t):
    return dt.datetime.strptime(t, TS_FORMAT).replace(tzinfo=dt.timezone.utc).timestamp()


def summarize(records):
    """records: dicts with 'sent' (epoch seconds), 'latency_ms', 'error'.

    Longest gap = largest time between the completion of one successful probe and the
    completion of the next successful probe (failed probes in between are inside the gap).
    """
    ok = [r for r in records if not r["error"]]
    errors = {}
    for r in records:
        if r["error"]:
            errors[r["error"]] = errors.get(r["error"], 0) + 1
    lat = [r["latency_ms"] for r in ok]
    gap, gap_from, gap_to = None, None, None
    done = [r["sent"] + r["latency_ms"] / 1000 for r in ok]
    for a, b in zip(done, done[1:]):
        if gap is None or b - a > gap:
            gap, gap_from, gap_to = b - a, a, b
    return {
        "count": len(records), "ok": len(ok), "errors_total": len(records) - len(ok),
        "errors_by_code": errors,
        "p50_ms": nearest_rank(lat, 50), "p99_ms": nearest_rank(lat, 99),
        "latency_basis": "successful probes only, nearest-rank",
        "longest_gap_s": None if gap is None else round(gap, 3),
        "longest_gap_from_utc": None if gap is None else _fmt(gap_from),
        "longest_gap_to_utc": None if gap is None else _fmt(gap_to),
        "gap_basis": "between completions of consecutive successful probes",
        "first_probe_utc": _fmt(records[0]["sent"]) if records else None,
        "last_probe_utc": _fmt(records[-1]["sent"]) if records else None,
    }


def run_probe(send, log, interval=5.0, stop=None, clock=time.time):
    """Probe until `stop` is set. `send()` performs one probe and raises on failure."""
    stop = stop or threading.Event()
    records = []
    next_tick = clock()
    while not stop.is_set():
        sent, started = clock(), time.perf_counter()
        error = None
        try:
            send()
        except Exception as err:  # noqa: BLE001 - every failure is recorded
            error = classify(err)
        latency_ms = (time.perf_counter() - started) * 1000
        records.append({"sent": sent, "latency_ms": latency_ms, "error": error})
        log.write(json.dumps({"t": _fmt(sent), "latency_ms": round(latency_ms, 2),
                              "error": error}) + "\n")
        log.flush()
        next_tick += interval
        now = clock()
        if next_tick < now and interval > 0:  # missed ticks are skipped, not replayed
            next_tick += math.ceil((now - next_tick) / interval) * interval
        stop.wait(max(0.0, next_tick - clock()))
    return records


def read_log(path):
    out = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                d = json.loads(line)
                out.append({"sent": _parse(d["t"]), "latency_ms": d["latency_ms"],
                            "error": d["error"]})
    return out


def main():
    ap = argparse.ArgumentParser(description="RETURN 1 availability probe.")
    ap.add_argument("--endpoint")
    ap.add_argument("--region")
    ap.add_argument("--port", type=int, default=8182)
    ap.add_argument("--interval", type=float, default=5.0, help="seconds between probes")
    ap.add_argument("--timeout", type=float, default=30.0, help="read timeout per probe, seconds")
    ap.add_argument("--log", help="JSON-lines file to write (required unless --summarize-only)")
    ap.add_argument("--summary-out", help="also write the summary to this JSON file")
    ap.add_argument("--summarize-only", metavar="LOG", help="print the summary of an existing log")
    a = ap.parse_args()

    if a.summarize_only:
        print(json.dumps(summarize(read_log(a.summarize_only)), indent=2))
        return
    if not (a.endpoint and a.region and a.log):
        ap.error("--endpoint, --region and --log are required")

    client = boto3.client(
        "neptunedata", endpoint_url=f"https://{a.endpoint}:{a.port}", region_name=a.region,
        config=Config(connect_timeout=5, read_timeout=a.timeout, retries={"max_attempts": 0}))
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    with open(a.log, "a") as log:
        records = run_probe(
            lambda: client.execute_open_cypher_query(openCypherQuery="RETURN 1 AS x"),
            log, a.interval, stop)
    summary = summarize(records)
    summary.update({"interval_s": a.interval, "timeout_s": a.timeout})
    print(json.dumps(summary, indent=2))
    if a.summary_out:
        with open(a.summary_out, "w") as f:
            json.dump(summary, f, indent=2)
    sys.exit(0)


if __name__ == "__main__":
    main()
