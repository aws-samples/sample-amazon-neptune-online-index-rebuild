"""
resume_monitor.py - Resume monitoring an in-flight index rebuild after run_reclaim.py
crashed on a client-side ReadTimeoutError.

Read-only against the cluster. Uses src/ functions unchanged; transient timeouts
on status polls are caught, logged, counted, and retried instead of aborting.
No concurrent workload. Each poll also records whether a trivial openCypher
query (RETURN 1) answers, so query availability during the rebuild is measured.

Merges into the original results.json so before/start data from the first run
are preserved.
"""

import argparse
import datetime as dt
import json
import os
import sys
import time

import boto3
from botocore.config import Config

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from storage_info import get_storage_info  # noqa: E402
from index_rebuild import get_index_rebuild_status  # noqa: E402
from verify import compare_storage, get_volume_bytes_used  # noqa: E402

GIB = 1024 ** 3
TERMINAL = {"COMPLETED", "FAILED", "CANCELLED"}


def now():
    return dt.datetime.now(dt.timezone.utc)


def iso(t):
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def log(msg):
    print(f"{iso(now())} {msg}", flush=True)


def probe_query(client):
    """Return (ok, seconds, error) for a trivial openCypher query."""
    s = time.time()
    try:
        client.execute_open_cypher_query(openCypherQuery="RETURN 1 AS x")
        return True, round(time.time() - s, 2), None
    except Exception as err:  # noqa: BLE001 - record any failure as-is
        return False, round(time.time() - s, 2), f"{type(err).__name__}: {str(err)[:200]}"


def cw_points(cluster_id, region, hours=6):
    pts = get_volume_bytes_used(cluster_id, region, lookback_hours=hours, period=300)
    return [{"t": iso(p["Timestamp"]), "gib": p["Average"] / GIB,
             "bytes": p["Average"]} for p in pts]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--region", required=True)
    ap.add_argument("--cluster-id", required=True)
    ap.add_argument("--results", default="/opt/run/results.json")
    ap.add_argument("--poll-seconds", type=int, default=30)
    ap.add_argument("--max-hours", type=float, default=4.0)
    ap.add_argument("--cw-settle-max-min", type=int, default=90)
    a = ap.parse_args()

    with open(a.results) as f:
        R = json.load(f)
    rebuild_id = R["rebuild_id"]
    t0 = dt.datetime.strptime(R["wall_start_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=dt.timezone.utc)
    R.setdefault("resume", {})
    res = R["resume"]
    res.update({"resumed_at": iso(now()), "polls": [], "status_poll_errors": [],
                "query_probes": []})

    def save():
        with open(a.results, "w") as f:
            json.dump(R, f, indent=2, default=str)

    probe_client = boto3.client(
        "neptunedata", endpoint_url=f"https://{a.endpoint}:8182", region_name=a.region,
        config=Config(connect_timeout=10, read_timeout=30, retries={"max_attempts": 1}))

    log(f"resuming monitor for {rebuild_id}; original start {R['wall_start_utc']}")
    deadline = now() + dt.timedelta(hours=a.max_hours)
    final = None
    while now() < deadline:
        try:
            st = get_index_rebuild_status(a.endpoint, a.region, rebuild_id)
        except Exception as err:  # noqa: BLE001 - transient; record and retry
            e = {"t": iso(now()), "error": f"{type(err).__name__}: {str(err)[:200]}"}
            res["status_poll_errors"].append(e)
            log(f"[status] poll failed, will retry: {e['error']}")
            save()
            time.sleep(a.poll_seconds)
            continue
        state = (st.get("status") or "").upper()
        poll = {"t": iso(now()), "status": state,
                "progressPercentage": st.get("progressPercentage"),
                "estimatedEndTime": st.get("estimatedEndTime")}
        res["polls"].append(poll)
        log(f"[status] {state} progress={poll['progressPercentage']} "
            f"eta={poll['estimatedEndTime']}")
        if state in TERMINAL:
            final = st
            break
        ok, secs, err = probe_query(probe_client)
        res["query_probes"].append({"t": iso(now()), "ok": ok, "seconds": secs, "error": err})
        log(f"[probe] RETURN 1 ok={ok} {secs}s {err or ''}")
        save()
        time.sleep(a.poll_seconds)

    t1 = now()
    if final is None:
        R["errors"].append(f"resume monitor hit max-hours ({a.max_hours}) without a terminal state")
        save()
        log("gave up waiting; no terminal state")
        sys.exit(3)

    final.pop("ResponseMetadata", None)
    R["final_status"] = final
    R["wall_end_utc"] = iso(t1)
    R["wall_duration_seconds"] = (t1 - t0).total_seconds()
    log(f"terminal {final.get('status')} at {iso(t1)}; wall duration "
        f"{R['wall_duration_seconds']:.0f}s; service startTime={final.get('startTime')} "
        f"endTime={final.get('endTime')} failureReason={final.get('failureReason')}")
    save()
    if final.get("status", "").upper() != "COMPLETED":
        sys.exit(2)

    ok, secs, err = probe_query(probe_client)
    res["post_rebuild_probe"] = {"t": iso(now()), "ok": ok, "seconds": secs, "error": err}
    log(f"[probe] post-rebuild RETURN 1 ok={ok} {secs}s {err or ''}")

    after = get_storage_info(a.endpoint, a.region)
    after.pop("ResponseMetadata", None)
    R["after_storage_info"] = after
    R["after_t"] = iso(now())
    print(compare_storage(R["before_storage_info"], after), flush=True)
    save()

    log("waiting for CloudWatch VolumeBytesUsed datapoints after rebuild end")
    cw_deadline = now() + dt.timedelta(minutes=a.cw_settle_max_min)
    R["cw_after"] = None
    while now() < cw_deadline:
        series = cw_points(a.cluster_id, a.region)
        later = [p for p in series if p["t"] >= iso(t1)]
        log(f"[cw] latest={series[-1] if series else None} post-rebuild points={len(later)}")
        if len(later) >= 2:
            R["cw_after"] = later[-1]
            R["cw_after_series"] = series
            break
        time.sleep(300)
    if R["cw_after"] is None:
        R["cw_after_series"] = cw_points(a.cluster_id, a.region)
        R["errors"].append(f"no 2 post-rebuild VolumeBytesUsed datapoints within "
                           f"{a.cw_settle_max_min} min")

    after2 = get_storage_info(a.endpoint, a.region)
    after2.pop("ResponseMetadata", None)
    R["after_settled_storage_info"] = after2
    R["after_settled_t"] = iso(now())
    save()
    log("DONE")


if __name__ == "__main__":
    main()
