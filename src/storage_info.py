"""
storage_info.py - Diagnose Amazon Neptune storage with the GetStorageInfo API.

Adds two health checks on top of the raw breakdown:
  * bytes per quad for each quad index (fragmentation signal), and
  * dictionary entries vs. live graph elements (dictionary-bloat signal).

GetStorageInfo counts are approximate and its sizes do not add up to the
CloudWatch VolumeBytesUsed metric. Use these numbers to compare components and
to estimate; use VolumeBytesUsed for billing and capacity planning.

Requires Neptune engine release 1.5.0.0 or later (1.5.0.1 recommended).
Docs: https://docs.aws.amazon.com/neptune/latest/userguide/storage-info.html
"""

import argparse
import datetime as dt
import json
import statistics
import time

import boto3

GIB = 1024 ** 3
MIB = 1024 ** 2
COMPACT_BYTES_PER_QUAD = 52  # measured on one PG dataset; confirm for your data
MIN_QUADS_FOR_BANDS = 1_000_000  # small indexes read high from page allocation
TOP_LEVEL = ("dictionaries", "undoLogs", "streams", "dfeStatistics", "schema", "internals")


def get_storage_info(endpoint, region, port=8182):
    client = boto3.client("neptunedata", endpoint_url=f"https://{endpoint}:{port}",
                          region_name=region)
    return client.get_storage_info()


def components(info):
    """Flatten a GetStorageInfo response to {component: sizeInBytes}.

    dictionaries.blobs is already part of dictionaries.total, so only the total
    is used. Quad indexes appear as quads.<PG|RDF>.<INDEX>.
    """
    out = {}
    for key in TOP_LEVEL:
        node = info.get(key)
        if isinstance(node, dict):
            leaf = node.get("total") if isinstance(node.get("total"), dict) else node
            if isinstance(leaf.get("sizeInBytes"), (int, float)):
                out[key] = leaf["sizeInBytes"]
    for model, indexes in (info.get("quads") or {}).items():
        for name, leaf in (indexes or {}).items():
            out[f"quads.{model}.{name}"] = leaf.get("sizeInBytes", 0)
    return out


def quad_health(info):
    """Bytes per quad for each quad index, with a rough reclaimable estimate.

    Bands (SPOG/POGS/GPSO/OSGP, indexes >= ~1M quads): ~50-60 compact; 60-90 normal
    with ongoing writes; 90-150 noticeable fragmentation; >150 heavy. PZSOG
    (geospatial) has no compacted reference value yet and is reported only.
    Estimate: reclaimable ~= sizeInBytes - count * 52.
    """
    rows = []
    for model, indexes in (info.get("quads") or {}).items():
        for name, leaf in (indexes or {}).items():
            size, count = leaf.get("sizeInBytes", 0), leaf.get("count", 0)
            bpq = size / count if count else None
            if name == "PZSOG" or bpq is None:
                band, est = "reported only (no reference value)", None
            elif count < MIN_QUADS_FOR_BANDS:
                band, est = "too small for the bands", None
            else:
                band = ("compact" if bpq < 60 else "normal" if bpq < 90
                        else "fragmented" if bpq < 150 else "heavily fragmented")
                est = max(0, size - count * COMPACT_BYTES_PER_QUAD)
            rows.append({"index": f"{model}.{name}", "sizeInBytes": size, "count": count,
                         "bytesPerQuad": bpq, "band": band, "estReclaimableBytes": est})
    return rows


def _client(endpoint, region, port=8182):
    return boto3.client("neptunedata", endpoint_url=f"https://{endpoint}:{port}",
                        region_name=region)


def _utc_now():
    return dt.datetime.now(dt.timezone.utc)


def _iso(t):
    return t.strftime("%Y-%m-%dT%H:%M:%SZ") if isinstance(t, dt.datetime) else t


def refresh_statistics(endpoint, region, port=8182, timeout_s=900, poll_s=15,
                       client=None, sleep=time.sleep):
    """Trigger a property-graph statistics refresh and wait until it is visible.

    Counts from the graph summary API come from Neptune's statistics, which can be
    stale. Needs neptune-db:ManageStatistics (and GetStatisticsStatus to wait). The
    refresh is asynchronous: this polls GetPropertygraphStatistics until it reports
    the statisticsId returned by the refresh, or a computation date at or after the
    request time. Returns a dict that records every poll; it never raises, so a
    missing permission or a timeout shows up as `completed: False` with `error` set.
    """
    client = client or _client(endpoint, region, port)
    requested = _utc_now()
    out = {"requestedAtUtc": _iso(requested), "statisticsId": None, "completed": False,
           "computedDate": None, "waitedSeconds": None, "error": None, "polls": []}
    started = time.monotonic()
    try:
        resp = client.manage_propertygraph_statistics(mode="refresh")
        out["statisticsId"] = (resp.get("payload") or {}).get("statisticsId")
        print(f"[stats] refresh requested at {out['requestedAtUtc']} "
              f"(statisticsId={out['statisticsId']})", flush=True)
        while True:
            st = (client.get_propertygraph_statistics().get("payload") or {})
            date = st.get("date")
            out["polls"].append({"t": _iso(_utc_now()), "statisticsId": st.get("statisticsId"),
                                 "date": _iso(date), "active": st.get("active")})
            same_id = out["statisticsId"] and st.get("statisticsId") == out["statisticsId"]
            fresh = isinstance(date, dt.datetime) and date >= requested
            if same_id or fresh:
                out["completed"], out["computedDate"] = True, _iso(date)
                break
            if time.monotonic() - started > timeout_s:
                out["error"] = f"timed out after {timeout_s}s waiting for the refresh"
                break
            sleep(poll_s)
    except Exception as err:  # noqa: BLE001 - recorded, not raised
        out["error"] = f"{type(err).__name__}: {str(err)[:300]}"
    out["waitedSeconds"] = round(time.monotonic() - started, 1)
    print(f"[stats] refresh {'completed' if out['completed'] else 'NOT confirmed'} "
          f"after {out['waitedSeconds']}s" + (f" ({out['error']})" if out["error"] else ""),
          flush=True)
    return out


def graph_summary_elements(endpoint, region, port=8182, client=None):
    """Live element counts from the graph summary API (PG: nodes+edges; RDF: subjects).

    These come from statistics that can be stale; `pg_stats_time` is the time they
    were last computed. Refresh first (--refresh-stats) if you need fresh numbers.
    """
    client = client or _client(endpoint, region, port)
    out = {}
    try:
        p = client.get_propertygraph_summary(mode="basic")["payload"]
        s = p["graphSummary"]
        out["pg_nodes"], out["pg_edges"] = s.get("numNodes", 0), s.get("numEdges", 0)
        out["pg_stats_time"] = _iso(p.get("lastStatisticsComputationTime"))
    except Exception as err:  # noqa: BLE001
        out["pg_error"] = repr(err)
    try:
        p = client.get_rdf_graph_summary(mode="basic")["payload"]
        out["rdf_subjects"] = p["graphSummary"].get("numDistinctSubjects", 0)
        out["rdf_stats_time"] = _iso(p.get("lastStatisticsComputationTime"))
    except Exception as err:  # noqa: BLE001
        out["rdf_error"] = repr(err)
    return out


DICTIONARY_CAVEAT = (
    "Entries per live element is a trend signal, not a dead-entry count. Each vertex or "
    "edge can need several dictionary entries (ids, string values), and Neptune keeps "
    "entries until dictionary garbage collection removes them, so a ratio above 1 is "
    "normal. Watch the trend over time: entries growing while live elements stay flat "
    "suggests entries that garbage collection could clean up. Counts are approximate "
    "and the live-element count comes from statistics that may need a refresh.")


def dictionary_health(info, summary):
    """Dictionary entries vs live property-graph elements, as a ratio.

    Returns entries, live_elements (nodes + edges), and entries_per_live_element
    (None when either count is missing). No percentage or dead-entry figure is
    given: see DICTIONARY_CAVEAT.
    """
    entries = ((info.get("dictionaries") or {}).get("total") or {}).get("count")
    live = summary.get("pg_nodes", 0) + summary.get("pg_edges", 0)
    ratio = entries / live if entries and live else None
    return {"entries": entries, "live_elements": live, "entries_per_live_element": ratio}


def dictionary_entries(info):
    return ((info.get("dictionaries") or {}).get("total") or {}).get("count")


def read_storage_repeatedly(read_fn, n=1, interval=60.0, sleep=None, clock=None):
    """Call read_fn() n times, `interval` seconds apart; returns [{capturedAtUtc, info}].

    GetStorageInfo counts are approximate and can move between calls even with no writes,
    so a snapshot can take several reads and use the median.
    """
    sleep = sleep or time.sleep
    clock = clock or _utc_now
    reads = []
    for i in range(n):
        if i:
            sleep(interval)
        reads.append({"capturedAtUtc": _iso(clock()), "info": read_fn()})
    return reads


def median_read(reads):
    """The read whose dictionary entry count is the median (the lower one for an even count).

    The returned read is a real GetStorageInfo response, so every other number in a snapshot
    comes from one consistent call. Reads without a dictionary count are ignored; if none
    has one, the middle read is returned.
    """
    counted = sorted((dictionary_entries(r["info"]), i) for i, r in enumerate(reads)
                     if dictionary_entries(r["info"]) is not None)
    if not counted:
        return reads[len(reads) // 2]
    return reads[counted[(len(counted) - 1) // 2][1]]


def dictionary_read_stats(reads):
    """All dictionary entry counts, their median, and the spread (max - min)."""
    counts = [dictionary_entries(r["info"]) for r in reads]
    known = [c for c in counts if c is not None]
    if not known:
        return {"reads": [{"capturedAtUtc": r["capturedAtUtc"], "entries": None} for r in reads],
                "median": None, "min": None, "max": None, "spreadAbs": None,
                "spreadPctOfMedian": None}
    med = statistics.median_low(known)
    spread = max(known) - min(known)
    return {"reads": [{"capturedAtUtc": r["capturedAtUtc"], "entries": c}
                      for r, c in zip(reads, counts)],
            "median": med, "min": min(known), "max": max(known), "spreadAbs": spread,
            "spreadPctOfMedian": (spread / med * 100) if med else None}


def build_health(info, summary, refresh=None, captured_at=None, reads=None):
    """Everything the --save-health file records, as one JSON-ready dict.

    With `reads` (from read_storage_repeatedly), `info` should be the median read's response;
    every read and the dictionary median/spread are recorded as well.
    """
    captured_at = captured_at or _iso(_utc_now())
    stats_time = summary.get("pg_stats_time")
    fresh = None
    if refresh and refresh.get("completed") and stats_time:
        fresh = stats_time >= refresh["requestedAtUtc"]  # ISO-8601 strings compare in order
    health = {
        "capturedAtUtc": captured_at,
        "totalStorageBytesUsed": info.get("totalStorageBytesUsed"),
        "components": components(info),
        "quadHealth": quad_health(info),
        "dictionary": dictionary_health(info, summary),
        "graphSummary": summary,
        "statisticsRefresh": refresh,
        "summaryStatisticsAreFreshAfterRefresh": fresh,
        "caveat": DICTIONARY_CAVEAT,
    }
    if reads:
        health["dictionaryReads"] = dictionary_read_stats(reads)
        health["storageReads"] = [
            {"capturedAtUtc": r["capturedAtUtc"],
             "totalStorageBytesUsed": r["info"].get("totalStorageBytesUsed"),
             "dictionaryEntries": dictionary_entries(r["info"]), "info": r["info"]}
            for r in reads]
        health["savedReadIsMedian"] = True
    return health


def summarize(info):
    lines = [f"{'Component':<28}{'Size':>12}", "-" * 40]
    for name, size in components(info).items():
        lines.append(f"{name:<28}{size / MIB:>9.1f} MiB")
    total = info.get("totalStorageBytesUsed")
    if total is not None:
        lines += ["-" * 40, f"{'totalStorageBytesUsed':<28}{total / GIB:>9.2f} GiB"]
    lines += ["", f"{'Quad index':<14}{'bytes/quad':>11}  band"]
    for r in quad_health(info):
        bpq = f"{r['bytesPerQuad']:.1f}" if r["bytesPerQuad"] else "-"
        lines.append(f"{r['index']:<14}{bpq:>11}  {r['band']}")
    if "streams" in info:
        lines.append("\nNote: streams hold storage until neptune_streams_expiry_days passes; "
                     "a rebuild does not touch them.")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser(description="Diagnose Neptune storage usage.")
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--region", required=True)
    ap.add_argument("--port", type=int, default=8182)
    ap.add_argument("--raw", action="store_true", help="print the raw JSON")
    ap.add_argument("--save", help="write the raw response to this JSON file (a baseline)")
    ap.add_argument("--dictionary-check", action="store_true",
                    help="compare dictionary entries with live elements (graph summary API)")
    ap.add_argument("--refresh-stats", action="store_true",
                    help="refresh property-graph statistics first and wait (needs "
                         "neptune-db:ManageStatistics); implies --dictionary-check")
    ap.add_argument("--save-health", metavar="FILE",
                    help="write bytes-per-quad, dictionary check, and statistics-refresh "
                         "details to this JSON file; implies --dictionary-check")
    ap.add_argument("--reads", type=int, default=1,
                    help="read GetStorageInfo this many times and use the median read "
                         "(counts are approximate and drift between calls); default 1")
    ap.add_argument("--read-interval", type=float, default=60.0,
                    help="seconds between reads when --reads > 1 (default 60)")
    a = ap.parse_args()
    if a.reads < 1:
        ap.error("--reads must be at least 1")
    check = a.dictionary_check or a.refresh_stats or bool(a.save_health)
    refresh = refresh_statistics(a.endpoint, a.region, a.port) if a.refresh_stats else None
    # live-element counts come right after the refresh, before the (possibly slow) reads
    summary = graph_summary_elements(a.endpoint, a.region, a.port) if check else None
    reads = read_storage_repeatedly(lambda: get_storage_info(a.endpoint, a.region, a.port),
                                    a.reads, a.read_interval)
    chosen = median_read(reads)
    info, captured_at = chosen["info"], reads[0]["capturedAtUtc"]
    if a.reads > 1:
        stats = dictionary_read_stats(reads)
        print(f"[reads] {a.reads} GetStorageInfo reads, {a.read_interval:.0f}s apart; dictionary "
              f"entries {[r['entries'] for r in stats['reads']]}, median {stats['median']}, "
              f"spread {stats['spreadAbs']} ({stats['spreadPctOfMedian']:.3f}% of median); "
              f"using the median read from {chosen['capturedAtUtc']}")
    if a.save:
        with open(a.save, "w") as f:
            json.dump(info, f, indent=2, default=str)
        print(f"saved {'median-read ' if a.reads > 1 else ''}baseline to {a.save}")
    print(json.dumps(info, indent=2, default=str) if a.raw else summarize(info))
    if check:
        d = dictionary_health(info, summary)
        ratio = d["entries_per_live_element"]
        print(f"\nDictionary entries: {d['entries']}  live elements (nodes + edges): "
              f"{d['live_elements']}  entries per live element: "
              f"{'-' if ratio is None else f'{ratio:.2f}'}")
        print(f"Statistics last computed: {summary.get('pg_stats_time')}")
        print(f"Note: {DICTIONARY_CAVEAT}")
        if a.save_health:
            with open(a.save_health, "w") as f:
                json.dump(build_health(info, summary, refresh, captured_at,
                                       reads if a.reads > 1 else None), f, indent=2, default=str)
            print(f"saved health report to {a.save_health}")


if __name__ == "__main__":
    main()
