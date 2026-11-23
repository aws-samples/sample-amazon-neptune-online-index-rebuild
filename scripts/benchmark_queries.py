"""
benchmark_queries.py - BLOG TEST HARNESS (not reusable library code).

Runs a fixed openCypher workload and records p50/p95/p99 latency, the writer's instance class,
and CloudWatch metrics for the run: BufferCacheHitRatio, FreeableMemory and VolumeReadIOPs
during the run, plus FreeableMemory and BufferCacheHitRatio for the 5 minutes BEFORE the run
(so a cold start can be shown to be cold). Run it before and after a rebuild, once cold (right
after a writer reboot) and once warm.

Two subcommands, so the warm pass can start straight after the cold pass and the CloudWatch
data (published a minute or two late) is collected afterwards:

  python scripts/benchmark_queries.py run --endpoint <writer> --region us-east-1 \
      --cluster-id <id> --instance-id <writer-instance-id> --chunks 3 \
      --vertices-per-chunk 2000000,2000000,1500000 --label before-cold --duration 600 \
      --defer-metrics --out results/bench-before-cold.json
  python scripts/benchmark_queries.py collect --region us-east-1 --file results/bench-before-cold.json

Without --defer-metrics, `run` waits --metrics-delay seconds and collects the metrics itself.

Fair before/after comparison: point lookups and two-hop queries pick uids ONLY from
vertices in buckets 6-9 (n % 10 in 6..9), which survive the 60% delete, in BOTH runs.
The pool is rebuilt from --chunks and --vertices-per-chunk using the same id stride as
stage_data_v2.py, and its size is recorded in the output JSON. With the same --seed the
query sequence is identical in every run.

The geo query is ST_Contains against one fixed polygon, which the Neptune docs say the
spatial index accelerates (ST_Distance is not documented as indexed).
"""

import argparse
import datetime as dt
import json
import os
import random
import statistics
import sys
import time

import boto3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from stage_data_v2 import CHUNK_ID_STRIDE  # noqa: E402

SURVIVING_BUCKETS = (6, 7, 8, 9)
PRE_WINDOW_MINUTES = 5
# A fixed box inside the area where stage_data_v2.py places :Place points
# (longitude -120..-70, latitude 25..48). x = longitude, y = latitude.
GEO_POLYGON_WKT = "POLYGON((-100 35, -90 35, -90 40, -100 40, -100 35))"


def parse_chunk_sizes(chunks, vertices_per_chunk):
    """Vertices loaded in each chunk 0..chunks-1.

    vertices_per_chunk is one number (used for every chunk) or a comma-separated list with
    one number per chunk, matching the --vertices used with stage_data_v2.py load.
    """
    sizes = [int(x) for x in str(vertices_per_chunk).split(",")]
    if len(sizes) == 1:
        sizes = sizes * chunks
    if len(sizes) != chunks:
        raise ValueError(f"expected 1 or {chunks} chunk sizes, got {len(sizes)}")
    for v in sizes:
        if v % 10 or not 0 < v <= CHUNK_ID_STRIDE:
            raise ValueError(f"chunk size {v} must be a multiple of 10 and at most {CHUNK_ID_STRIDE}")
    return sizes


def uid_pool_size(sizes):
    """Number of vertices in buckets 6-9 across the loaded chunks."""
    return sum((v // 10) * len(SURVIVING_BUCKETS) for v in sizes)


def uid_number(k, sizes):
    """The k-th (0-based) vertex number in the surviving-bucket pool."""
    for chunk, v in enumerate(sizes):
        per_chunk = (v // 10) * len(SURVIVING_BUCKETS)
        if k < per_chunk:
            return chunk * CHUNK_ID_STRIDE + (k // len(SURVIVING_BUCKETS)) * 10 \
                + SURVIVING_BUCKETS[k % len(SURVIVING_BUCKETS)]
        k -= per_chunk
    raise IndexError("k is outside the uid pool")


def uid_string(n):
    return f"u-{n:012d}"


QUERIES = {
    "point_lookup": ("MATCH (n:Item {uid: $uid}) RETURN n.name AS name", ("uid",)),
    "range_scan": ("MATCH (n:Item) WHERE n.bucket = $b AND n.k0 > $lo RETURN n.uid LIMIT 500",
                   ("b", "lo")),
    "two_hop": ("MATCH (n:Item {uid: $uid})-[:LINK]->()-[:LINK]->(m) RETURN count(m) AS c",
                ("uid",)),
    "geo_within": ("MATCH (n:Place) WHERE ST_Contains(ST_GeomFromText('POLYGON((-100 35, -90 35, -90 40, -100 40, -100 35))'), n.loc) RETURN count(n) AS c", ()),
}


# Optional fifth query (--wide-scan). It reads far more pages than the other four: up to 50,000
# Item rows from one of buckets 6-9 (they survive the 60% delete, so the result size is the
# same before and after). k0 is uniform in [0, 2^40) and lo is drawn below 2^38, so at least
# 75% of the rows in the bucket match and LIMIT 50000 is always reached (a bucket holds
# ~550,000 Items). It is NOT part of QUERIES and draws from its own random generator, so the
# other four queries keep exactly the sequence they had without --wide-scan.
WIDE_LIMIT = 50000
WIDE_QUERY = ("MATCH (n:Item) WHERE n.bucket = $b AND n.k0 > $lo "
              f"RETURN n.uid AS uid LIMIT {WIDE_LIMIT}")
WIDE_LO_MAX = 1 << 38
WIDE_EVERY = 20  # one wide scan after every 20 regular queries (~5% of queries)


def make_params(rnd, needed, pool, sizes):
    """Draw only the parameters a query uses (the order of draws is fixed)."""
    params = {}
    if "uid" in needed:
        params["uid"] = uid_string(uid_number(rnd.randrange(pool), sizes))
    if "b" in needed:
        params["b"] = rnd.choice(SURVIVING_BUCKETS)
    if "lo" in needed:
        params["lo"] = rnd.randrange(1 << 39)
    return params


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))] if xs else None


def _iso(t):
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


# ------------------------------------------------------------------ CloudWatch
def metric_points(cw, metric, dims, start, end, stats=("Average",)):
    """One point per minute: {"t": <UTC>, "average": ..., ...} for each requested statistic."""
    r = cw.get_metric_statistics(Namespace="AWS/Neptune", MetricName=metric, Dimensions=dims,
                                 StartTime=start, EndTime=end, Period=60, Statistics=list(stats))
    return [{"t": _iso(p["Timestamp"]), **{s.lower(): p[s] for s in stats}}
            for p in sorted(r.get("Datapoints", []), key=lambda p: p["Timestamp"])]


def mean_of(points, key="average"):
    vals = [p[key] for p in points]
    return statistics.mean(vals) if vals else None


def collect_metrics(cw, instance_id, cluster_id, start, end):
    """CloudWatch data for a pass that ran from `start` to `end` (timezone-aware datetimes).

    preWindow: FreeableMemory and BufferCacheHitRatio for the PRE_WINDOW_MINUTES before the
    run. duringRun: the same two plus the cluster's VolumeReadIOPs. VolumeReadIOPs is
    published in coarser bins than one minute, so only compare passes of equal duration.
    """
    inst = [{"Name": "DBInstanceIdentifier", "Value": instance_id}]
    clus = [{"Name": "DBClusterIdentifier", "Value": cluster_id}]
    pre_start = start - dt.timedelta(minutes=PRE_WINDOW_MINUTES)
    pre = {"startUtc": _iso(pre_start), "endUtc": _iso(start),
           "FreeableMemory": metric_points(cw, "FreeableMemory", inst, pre_start, start,
                                           ("Average", "Minimum")),
           "BufferCacheHitRatio": metric_points(cw, "BufferCacheHitRatio", inst, pre_start, start)}
    during = {"startUtc": _iso(start), "endUtc": _iso(end),
              "FreeableMemory": metric_points(cw, "FreeableMemory", inst, start, end,
                                              ("Average", "Minimum")),
              "BufferCacheHitRatio": metric_points(cw, "BufferCacheHitRatio", inst, start, end),
              "VolumeReadIOPs": metric_points(cw, "VolumeReadIOPs", clus, start, end,
                                              ("Average", "Sum"))}
    return {"preWindow": pre, "duringRun": during,
            "BufferCacheHitRatio": mean_of(during["BufferCacheHitRatio"]),
            "VolumeReadIOPs": mean_of(during["VolumeReadIOPs"])}


# ------------------------------------------------------------------ the pass
def execute_pass(client, sizes, seed, duration, skip_geo=False, clock=time.time,
                 perf=time.perf_counter, wide_scan=False, wide_every=WIDE_EVERY):
    """Run the fixed workload for `duration` seconds. Returns latencies, empties, errors.

    With wide_scan=True, one extra "wide_scan" query runs after every `wide_every` regular
    queries (its own random generator; the regular sequence is unchanged).
    """
    pool = uid_pool_size(sizes)
    rnd = random.Random(seed)  # same query sequence every run
    names = [k for k in QUERIES if not (skip_geo and k == "geo_within")]
    lat = {k: [] for k in QUERIES}
    empty = {k: 0 for k in QUERIES}
    if wide_scan:
        lat["wide_scan"], empty["wide_scan"] = [], 0
        wide_rnd = random.Random(seed + 1000003)
    done = 0
    errors = {}
    start = dt.datetime.now(dt.timezone.utc)
    stop = clock() + duration
    while clock() < stop:
        if wide_scan and done and done % wide_every == 0:
            name, text = "wide_scan", WIDE_QUERY
            params = {"b": wide_rnd.choice(SURVIVING_BUCKETS),
                      "lo": wide_rnd.randrange(WIDE_LO_MAX)}
        else:
            name = rnd.choice(names)
            text, needed = QUERIES[name]
            params = make_params(rnd, needed, pool, sizes)
        done += 1
        t0 = perf()
        try:
            resp = client.execute_open_cypher_query(
                openCypherQuery=text, **({"parameters": json.dumps(params)} if params else {}))
            lat[name].append((perf() - t0) * 1000)
            if not resp.get("results"):
                empty[name] += 1
        except Exception as err:  # noqa: BLE001
            code = getattr(err, "response", {}).get("Error", {}).get("Code") or type(err).__name__
            errors[code] = errors.get(code, 0) + 1
    return lat, empty, errors, start, dt.datetime.now(dt.timezone.utc)


def build_result(a, sizes, instance, lat, empty, errors, start, end):
    pool = uid_pool_size(sizes)
    out = {"label": a.label, "region": a.region, "clusterId": a.cluster_id,
           "instanceId": a.instance_id,
           "instanceClass": instance["DBInstanceClass"], "instanceStatus": instance["DBInstanceStatus"],
           "start": start.isoformat(), "end": end.isoformat(),
           "startUtc": _iso(start), "endUtc": _iso(end),
           "durationSeconds": a.duration, "seed": a.seed, "skipGeo": a.skip_geo,
           "wideScan": bool(getattr(a, "wide_scan", False)),
           "wideEvery": getattr(a, "wide_every", None), "wideQuery": WIDE_QUERY,
           "chunks": len(sizes), "verticesPerChunk": sizes,
           "uidPool": {"size": pool, "survivingBuckets": list(SURVIVING_BUCKETS),
                       "idStride": CHUNK_ID_STRIDE,
                       "firstUid": uid_string(uid_number(0, sizes)),
                       "lastUid": uid_string(uid_number(pool - 1, sizes))},
           "geoPolygonWkt": GEO_POLYGON_WKT, "errors": errors, "queries": {},
           "metricsCollected": False}
    for k, xs in lat.items():
        out["queries"][k] = {"n": len(xs), "emptyResults": empty[k], "p50_ms": pct(xs, 50),
                             "p95_ms": pct(xs, 95), "p99_ms": pct(xs, 99)}
    return out


def collect_into(path, region, cw=None):
    """Add the CloudWatch data to a benchmark JSON written with --defer-metrics."""
    with open(path) as f:
        out = json.load(f)
    start, end = (dt.datetime.fromisoformat(out[k]) for k in ("start", "end"))
    cw = cw or boto3.client("cloudwatch", region_name=region)
    out.update(collect_metrics(cw, out["instanceId"], out["clusterId"], start, end))
    out["metricsCollected"] = True
    out["metricsCollectedAtUtc"] = _iso(dt.datetime.now(dt.timezone.utc))
    with open(path, "w") as f:
        json.dump(out, f, indent=2)
    return out


def main(argv=None):
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="run one pass")
    r.add_argument("--endpoint", required=True)
    r.add_argument("--region", required=True)
    r.add_argument("--cluster-id", required=True)
    r.add_argument("--instance-id", required=True)
    r.add_argument("--label", required=True)
    r.add_argument("--duration", type=int, default=600)
    r.add_argument("--chunks", type=int, required=True, help="chunks loaded (chunk numbers 0..N-1)")
    r.add_argument("--vertices-per-chunk", required=True,
                   help="the --vertices value used with stage_data_v2.py load: one number for all "
                        "chunks, or a comma-separated list with one per chunk "
                        "(for example 2000000,2000000,1500000)")
    r.add_argument("--seed", type=int, default=7)
    r.add_argument("--skip-geo", action="store_true")
    r.add_argument("--wide-scan", action="store_true",
                   help="also run wide_scan (LIMIT 50000) after every --wide-every queries")
    r.add_argument("--wide-every", type=int, default=WIDE_EVERY)
    r.add_argument("--defer-metrics", action="store_true",
                   help="write the JSON right away; collect CloudWatch data later with `collect`")
    r.add_argument("--metrics-delay", type=int, default=120,
                   help="seconds to wait before collecting metrics (ignored with --defer-metrics)")
    r.add_argument("--out", required=True)
    c = sub.add_parser("collect", help="add CloudWatch data to a --defer-metrics result")
    c.add_argument("--region", required=True)
    c.add_argument("--file", required=True)
    a = ap.parse_args(argv)

    if a.cmd == "collect":
        out = collect_into(a.file, a.region)
        print(json.dumps({k: out[k] for k in ("label", "instanceClass", "BufferCacheHitRatio",
                                              "VolumeReadIOPs", "metricsCollectedAtUtc")}, indent=2))
        return

    try:
        sizes = parse_chunk_sizes(a.chunks, a.vertices_per_chunk)
    except ValueError as err:
        ap.error(str(err))
    instance = boto3.client("neptune", region_name=a.region).describe_db_instances(
        DBInstanceIdentifier=a.instance_id)["DBInstances"][0]
    print(f"instance {a.instance_id}: {instance['DBInstanceClass']} ({instance['DBInstanceStatus']})",
          flush=True)
    client = boto3.client("neptunedata", endpoint_url=f"https://{a.endpoint}:8182",
                          region_name=a.region)
    lat, empty, errors, start, end = execute_pass(
        client, sizes, a.seed, a.duration, a.skip_geo, wide_scan=a.wide_scan, wide_every=a.wide_every)
    out = build_result(a, sizes, instance, lat, empty, errors, start, end)
    with open(a.out, "w") as f:
        json.dump(out, f, indent=2)
    if not a.defer_metrics:
        time.sleep(a.metrics_delay)  # let the 1-minute metrics publish
        out = collect_into(a.out, a.region)
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
