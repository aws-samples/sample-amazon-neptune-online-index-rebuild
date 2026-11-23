"""
stage_data_v2.py - BLOG TEST HARNESS (not reusable library code).

Builds a fragmented graph that also exercises the dictionary and the geospatial
index, so the re-test can show dictionary bloat, PZSOG, and streams effects:
  * every vertex and edge has a unique string id AND a unique string property
    (uid, name), so each one adds dictionary entries;
  * an `update` step rewrites string properties to new unique values, leaving
    the old values as dead dictionary entries;
  * a `geo` step creates :Place vertices with Geometry POINT properties so the
    PZSOG index is populated;
  * `delete` removes 60% of :Item vertices (and their edges).

Subcommands: load | geo | update | delete
"""

import argparse
import csv
import json
import os
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import boto3

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from query_retry import run_with_retry  # noqa: E402
from storage_info import get_storage_info  # noqa: E402

GIB = 1024 ** 3

# Chunk k owns the id range [k * CHUNK_ID_STRIDE, k * CHUNK_ID_STRIDE + vertices).
# A fixed stride (not the chunk size) keeps chunks from colliding even if --vertices
# differs between runs. It is a multiple of 10, so bucket = n % 10 for every vertex.
# benchmark_queries.py imports this constant to rebuild the uid pool.
CHUNK_ID_STRIDE = 10_000_000


def log(msg):
    print(f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} {msg}", flush=True)


def ndata(a):
    return boto3.client("neptunedata", endpoint_url=f"https://{a.endpoint}:8182",
                        region_name=a.region)


def write_chunk(workdir, chunk, nv, ne, seed):
    if not 0 < nv <= CHUNK_ID_STRIDE or nv % 10:
        raise ValueError(f"--vertices must be a multiple of 10 and at most {CHUNK_ID_STRIDE}")
    rnd = random.Random(seed)
    base = chunk * CHUNK_ID_STRIDE
    vpath, epath = (os.path.join(workdir, f"{k}_{chunk:03d}.csv") for k in ("v", "e"))
    with open(vpath, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["~id", "~label", "uid:String", "name:String", "bucket:Int"]
                   + [f"k{i}:Long" for i in range(4)])
        for i in range(nv):
            n = base + i
            w.writerow([f"item-{n:012d}", "Item", f"u-{n:012d}",
                        f"name-{rnd.getrandbits(64):016x}", i % 10]
                       + [rnd.randrange(1 << 40) for _ in range(4)])
    with open(epath, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["~id", "~from", "~to", "~label", "tag:String"])
        for j in range(ne):
            w.writerow([f"link-{chunk:03d}-{j:010d}", f"item-{base + rnd.randrange(nv):012d}",
                        f"item-{base + rnd.randrange(nv):012d}", "LINK",
                        f"t-{rnd.getrandbits(48):012x}"])
    return vpath, epath


def wait_load(client, load_id):
    while True:
        st = client.get_loader_job_status(loadId=load_id)["payload"]["overallStatus"]
        log(f"[load] {load_id} {st.get('status')} records={st.get('totalRecords')}")
        if st["status"] == "LOAD_COMPLETED":
            return
        if st["status"] not in ("LOAD_IN_PROGRESS", "LOAD_NOT_STARTED", "LOAD_IN_QUEUE"):
            raise RuntimeError(f"load {load_id} ended with {st['status']}")
        time.sleep(30)


def cmd_load(a):
    s3, client = boto3.client("s3", region_name=a.region), ndata(a)
    os.makedirs(a.workdir, exist_ok=True)
    for chunk in range(a.start_chunk, a.start_chunk + a.chunks):
        base = chunk * CHUNK_ID_STRIDE
        log(f"[load] chunk {chunk}: vertex numbers {base}..{base + a.vertices - 1} "
            f"(uid u-{base:012d}..u-{base + a.vertices - 1:012d})")
        paths = write_chunk(a.workdir, chunk, a.vertices, a.edges, seed=chunk)
        prefix = f"v2/chunk_{chunk:03d}/"
        for p in paths:
            s3.upload_file(p, a.bucket, prefix + os.path.basename(p))
            os.remove(p)
        lid = client.start_loader_job(source=f"s3://{a.bucket}/{prefix}", format="csv",
                                      s3BucketRegion=a.region, iamRoleArn=a.loader_role,
                                      failOnError=True, parallelism="OVERSUBSCRIBE")["payload"]["loadId"]
        wait_load(client, lid)
        total = get_storage_info(a.endpoint, a.region).get("totalStorageBytesUsed", 0)
        log(f"[load] chunk {chunk} done; totalStorageBytesUsed={total / GIB:.2f} GiB")


def _batched(a, query, params_fn, label, keys):
    client, lock, done = ndata(a), threading.Lock(), {"n": 0}

    def worker(k):
        while True:
            r = run_with_retry(lambda: client.execute_open_cypher_query(
                openCypherQuery=query, parameters=json.dumps(params_fn(k))))
            c = r["results"][0]["c"]
            with lock:
                done["n"] += c
            if c == 0:
                return
    with ThreadPoolExecutor(max_workers=a.workers) as ex:
        list(ex.map(worker, keys))
    log(f"[{label}] done: {done['n']}")


def cmd_geo(a):
    # Verify the spatial syntax against the Neptune spatial-data docs before running.
    client, rnd = ndata(a), random.Random(11)
    q = ("UNWIND $rows AS r CREATE (:Place {pid: r.pid, loc: ST_GeomFromText(r.wkt)}) "
         "RETURN count(*) AS c")
    for start in range(0, a.places, 1000):
        rows = [{"pid": f"p-{i:010d}", "wkt": f"POINT({rnd.uniform(-120, -70):.5f} "
                 f"{rnd.uniform(25, 48):.5f})"} for i in range(start, min(a.places, start + 1000))]
        run_with_retry(lambda: client.execute_open_cypher_query(
            openCypherQuery=q, parameters=json.dumps({"rows": rows})))
    log(f"[geo] created {a.places} :Place vertices with POINT geometry")


def cmd_update(a):
    # Rewrite name on surviving buckets to NEW unique strings -> old values become dead
    # dictionary entries. Marks rows with a round number so batches terminate.
    q = ("MATCH (n:Item) WHERE n.bucket = $b AND coalesce(n.rev, 0) < $rev "
         "WITH n LIMIT $lim SET n.name = n.uid + '-r' + toString($rev), n.rev = $rev "
         "RETURN count(*) AS c")
    _batched(a, q, lambda b: {"b": b, "rev": a.rev, "lim": a.batch}, "update", [6, 7, 8, 9])


def cmd_delete(a):
    q = "MATCH (n:Item) WHERE n.bucket = $b WITH n LIMIT $lim DETACH DELETE n RETURN count(*) AS c"
    _batched(a, q, lambda b: {"b": b, "lim": a.batch}, "delete", [0, 1, 2, 3, 4, 5])


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--endpoint", required=True)
    p.add_argument("--region", required=True)
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--batch", type=int, default=2000)
    sub = p.add_subparsers(dest="cmd", required=True)
    l = sub.add_parser("load")
    l.add_argument("--bucket", required=True)
    l.add_argument("--loader-role", required=True)
    l.add_argument("--workdir", default="/opt/run/data")
    l.add_argument("--chunks", type=int, default=6, help="how many chunks to load in this run")
    l.add_argument("--start-chunk", type=int, default=0,
                   help="first chunk number; use 1, 2, ... to add chunks one at a time "
                        "without re-loading earlier ones")
    l.add_argument("--vertices", type=int, default=2_000_000)
    l.add_argument("--edges", type=int, default=8_000_000)
    g = sub.add_parser("geo")
    g.add_argument("--places", type=int, default=500_000)
    u = sub.add_parser("update")
    u.add_argument("--rev", type=int, default=1)
    sub.add_parser("delete")
    a = p.parse_args()
    {"load": cmd_load, "geo": cmd_geo, "update": cmd_update, "delete": cmd_delete}[a.cmd](a)


if __name__ == "__main__":
    main()
