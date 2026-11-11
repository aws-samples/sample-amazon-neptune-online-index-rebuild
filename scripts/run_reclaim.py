"""
run_reclaim.py - Instrumented diagnose -> rebuild -> monitor -> verify run.

Wraps the repo's src/ functions (unchanged) and adds what the blog needs:
  * UTC wall-clock timing of the rebuild
  * every status poll logged with progressPercentage
  * a light concurrent openCypher read/update workload during the rebuild,
    each query wrapped in src.index_rebuild.with_backoff
  * CancelledByMaintenanceException accounting, two ways:
      - raw: a botocore after-call hook counts every error code returned on
        every neptunedata HTTP response in this process (load + status polls)
      - with_backoff: per-call counts of how often the exception was raised
        inside with_backoff, how many were retried, and how many exhausted
  * GetStorageInfo before/after + CloudWatch VolumeBytesUsed before/after

All numbers are written as-measured to a JSON file; nothing is estimated.
"""

import argparse
import collections
import datetime as dt
import json
import os
import random
import sys
import threading
import time

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from storage_info import get_storage_info, summarize  # noqa: E402
from index_rebuild import (  # noqa: E402
    RETRIABLE_ERRORS, get_index_rebuild_status, start_index_rebuild, with_backoff)
from verify import compare_storage, get_volume_bytes_used  # noqa: E402

GIB = 1024 ** 3
TERMINAL = {"COMPLETED", "FAILED", "CANCELLED"}
CBM = "CancelledByMaintenanceException"


def now():
    return dt.datetime.now(dt.timezone.utc)


def iso(t):
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def log(msg):
    print(f"{iso(now())} {msg}", flush=True)


# ------------------------------------------------ raw error accounting hook
RAW_ERRORS = collections.Counter()
RAW_LOCK = threading.Lock()


def _after_call(http_response=None, parsed=None, model=None, **_):
    code = (parsed or {}).get("Error", {}).get("Code")
    if code:
        with RAW_LOCK:
            RAW_ERRORS[code] += 1
        log(f"[raw-error] {model.name if model else '?'} -> {code} "
            f"(HTTP {getattr(http_response, 'status_code', '?')})")


boto3.setup_default_session()
boto3.DEFAULT_SESSION.events.register("after-call.neptunedata", _after_call)


# ------------------------------------------------ concurrent workload
class Workload:
    READ_Q = "MATCH (n:Item) WHERE n.bucket = $b RETURN n.k0 AS k LIMIT 20"
    UPDATE_Q = ("MATCH (n:Item) WHERE n.bucket = $b WITH n SKIP $s LIMIT 1 "
                "SET n.k1 = $v RETURN count(*) AS c")

    def __init__(self, endpoint, region, threads=2, pause=0.25):
        self.client = boto3.client(
            "neptunedata", endpoint_url=f"https://{endpoint}:8182", region_name=region)
        self.threads = threads
        self.pause = pause
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.stats = collections.Counter()
        self.non_retriable = collections.Counter()
        self.workers = []

    def _call(self, query, params):
        raised = collections.Counter()

        def fn():
            try:
                return self.client.execute_open_cypher_query(
                    openCypherQuery=query, parameters=json.dumps(params))
            except ClientError as err:
                raised[err.response.get("Error", {}).get("Code", "")] += 1
                raise

        try:
            with_backoff(fn)
            ok = True
            final_code = None
        except ClientError as err:
            ok = False
            final_code = err.response.get("Error", {}).get("Code", "")
        except Exception as err:  # timeouts, connection errors
            ok = False
            final_code = type(err).__name__
        with self.lock:
            self.stats["queries"] += 1
            self.stats["succeeded" if ok else "failed"] += 1
            for code, n in raised.items():
                self.stats[f"raised:{code}"] += n
                if code in RETRIABLE_ERRORS:
                    # with_backoff retries every retriable raise except an
                    # exhausting final attempt.
                    retried = n - (1 if final_code == code else 0)
                    self.stats[f"retried_by_with_backoff:{code}"] += retried
            if final_code:
                if final_code in RETRIABLE_ERRORS:
                    self.stats[f"exhausted:{final_code}"] += 1
                else:
                    self.non_retriable[final_code] += 1

    def _loop(self, idx):
        rnd = random.Random(idx)
        while not self.stop.is_set():
            b = rnd.choice([6, 7, 8, 9])  # surviving buckets
            if rnd.random() < 0.7:
                self._call(self.READ_Q, {"b": b})
                with self.lock:
                    self.stats["reads"] += 1
            else:
                self._call(self.UPDATE_Q, {"b": b, "s": rnd.randrange(1000),
                                           "v": rnd.randrange(1 << 40)})
                with self.lock:
                    self.stats["updates"] += 1
            time.sleep(self.pause)

    def start(self):
        for i in range(self.threads):
            t = threading.Thread(target=self._loop, args=(i,), daemon=True)
            t.start()
            self.workers.append(t)

    def finish(self):
        self.stop.set()
        for t in self.workers:
            t.join(timeout=120)
        return dict(self.stats), dict(self.non_retriable)


# ------------------------------------------------ helpers
def cw_points(cluster_id, region, hours=6):
    pts = get_volume_bytes_used(cluster_id, region, lookback_hours=hours, period=300)
    return [{"t": iso(p["Timestamp"]), "gib": p["Average"] / GIB,
             "bytes": p["Average"]} for p in pts]


def components(info):
    out = {"totalStorageBytesUsed": info.get("totalStorageBytesUsed")}
    for k in ("dictionaries", "undoLogs", "streams", "dfeStatistics",
              "schema", "internals"):
        v = info.get(k)
        if isinstance(v, dict):
            out[k] = (v.get("total") or v).get("sizeInBytes")
    quads = info.get("quads") or {}
    for model, idx in quads.items():
        for name, v in (idx or {}).items():
            out[f"quads.{model}.{name}"] = v.get("sizeInBytes")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--region", required=True)
    ap.add_argument("--cluster-id", required=True)
    ap.add_argument("--scope", default="QUADS", choices=("ALL", "QUADS", "DICTIONARIES"))
    ap.add_argument("--poll-seconds", type=int, default=30)
    ap.add_argument("--load-threads", type=int, default=2)
    ap.add_argument("--cw-settle-max-min", type=int, default=90)
    ap.add_argument("--out", default="/opt/run/results.json")
    a = ap.parse_args()

    R = {"cluster_id": a.cluster_id, "region": a.region, "scope": a.scope,
         "polls": [], "errors": []}

    def save():
        R["raw_error_counts"] = dict(RAW_ERRORS)
        with open(a.out, "w") as f:
            json.dump(R, f, indent=2, default=str)

    rds = boto3.client("neptune", region_name=a.region)
    c = rds.describe_db_clusters(DBClusterIdentifier=a.cluster_id)["DBClusters"][0]
    R["engine_version"] = c["EngineVersion"]
    log(f"engine version {c['EngineVersion']}")

    # 1. BASELINE
    log("=== Step 1: baseline ===")
    before = get_storage_info(a.endpoint, a.region)
    print(summarize(before), flush=True)
    log(f"raw totalStorageBytesUsed BEFORE = {before.get('totalStorageBytesUsed')}")
    R["before_storage_info"] = before
    R["before_components"] = components(before)
    R["cw_before_series"] = cw_points(a.cluster_id, a.region)
    R["cw_before"] = R["cw_before_series"][-1] if R["cw_before_series"] else None
    log(f"CloudWatch VolumeBytesUsed BEFORE = {R['cw_before']}")
    save()

    # 2-4. START, REBUILD, MONITOR (with concurrent load)
    wl = Workload(a.endpoint, a.region, threads=a.load_threads)
    wl.start()
    log("concurrent workload started")
    t0 = now()
    R["wall_start_utc"] = iso(t0)
    log(f"=== Step 2: start_index_rebuild scope={a.scope} at {iso(t0)} ===")
    try:
        rebuild_id = start_index_rebuild(a.endpoint, a.region, scope=a.scope)
    except Exception as err:
        R["errors"].append(f"start_index_rebuild: {err!r}")
        wl.finish()
        save()
        raise
    R["rebuild_id"] = rebuild_id
    log(f"rebuildId={rebuild_id}")
    save()

    log("=== Step 3: monitor ===")
    final = None
    while True:
        try:
            st = get_index_rebuild_status(a.endpoint, a.region, rebuild_id)
        except Exception as err:
            R["errors"].append(f"{iso(now())} get_index_rebuild_status: {err!r}")
            log(f"[status] ERROR {err!r}")
            save()
            raise
        state = (st.get("status") or "").upper()
        poll = {"t": iso(now()), "status": state,
                "progressPercentage": st.get("progressPercentage"),
                "estimatedEndTime": st.get("estimatedEndTime")}
        R["polls"].append(poll)
        log(f"[status] {rebuild_id}: {state} progress={poll['progressPercentage']} "
            f"eta={poll['estimatedEndTime']}")
        if state in TERMINAL:
            final = st
            break
        save()
        time.sleep(a.poll_seconds)
    t1 = now()
    stats, non_retriable = wl.finish()
    log("concurrent workload stopped")

    R["wall_end_utc"] = iso(t1)
    R["wall_duration_seconds"] = (t1 - t0).total_seconds()
    R["final_status"] = final
    R["workload_stats"] = stats
    R["workload_non_retriable_errors"] = non_retriable
    log(f"=== Step 5: end {iso(t1)}; duration {R['wall_duration_seconds']:.0f}s; "
        f"final={final.get('status')} failureReason={final.get('failureReason')} ===")
    save()

    if final.get("status", "").upper() != "COMPLETED":
        log("rebuild did not complete; skipping after/verify")
        save()
        sys.exit(2)

    # 6. AFTER
    log("=== Step 6: after snapshot ===")
    after = get_storage_info(a.endpoint, a.region)
    print(summarize(after), flush=True)
    print(compare_storage(before, after), flush=True)
    R["after_storage_info"] = after
    R["after_components"] = components(after)
    save()

    # 7. VERIFY - wait for a CloudWatch datapoint after the rebuild ended.
    log("=== Step 7: CloudWatch settle ===")
    deadline = now() + dt.timedelta(minutes=a.cw_settle_max_min)
    R["cw_after"] = None
    while now() < deadline:
        series = cw_points(a.cluster_id, a.region)
        later = [p for p in series if p["t"] >= iso(t1)]
        log(f"[cw] latest={series[-1] if series else None} post-rebuild points={len(later)}")
        # Need at least two post-rebuild points so the reading has settled.
        if len(later) >= 2:
            R["cw_after"] = later[-1]
            R["cw_after_series"] = series
            break
        time.sleep(300)
    if R["cw_after"] is None:
        R["cw_after_series"] = cw_points(a.cluster_id, a.region)
        R["errors"].append("no post-rebuild VolumeBytesUsed datapoints within "
                           f"{a.cw_settle_max_min} min")

    after2 = get_storage_info(a.endpoint, a.region)
    R["after_settled_storage_info"] = after2
    R["after_settled_components"] = components(after2)
    R["after_settled_t"] = iso(now())
    save()
    log("DONE")


if __name__ == "__main__":
    main()
