"""
stage_data.py - Build a deliberately fragmented Neptune graph for the blog demo.

This is a CONTROLLED EXAMPLE, not a production benchmark:
  1. `load`   - generate Gremlin CSV chunks, upload to S3, bulk-load them, and
                repeat until GetStorageInfo reports >= --target-gib.
  2. `delete` - DETACH DELETE every :Item vertex whose `bucket` is 0..5
                (60% of vertices, plus their edges) in small batches.

Run on the EC2 runner inside the VPC. Uses the repo's src/ helpers unchanged.
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
from storage_info import get_storage_info  # noqa: E402
from index_rebuild import with_backoff  # noqa: E402

GIB = 1024 ** 3
VOCAB = [f"cat{i:02d}" for i in range(50)]  # low cardinality -> small dictionary


def log(msg):
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print(f"{ts} {msg}", flush=True)


def ndata(endpoint, region, port=8182):
    return boto3.client(
        "neptunedata", endpoint_url=f"https://{endpoint}:{port}", region_name=region
    )


# ----------------------------------------------------------------- load
def write_chunk(workdir, chunk, n_vertices, n_edges, seed):
    rnd = random.Random(seed)
    vpath = os.path.join(workdir, f"vertices_{chunk:03d}.csv")
    epath = os.path.join(workdir, f"edges_{chunk:03d}.csv")
    with open(vpath, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["~id", "~label", "bucket:Int"]
                   + [f"k{i}:Long" for i in range(8)] + ["cat:String"])
        for i in range(n_vertices):
            w.writerow([f"v{chunk}_{i}", "Item", i % 10]
                       + [rnd.randrange(1 << 40) for _ in range(8)]
                       + [rnd.choice(VOCAB)])
    with open(epath, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["~id", "~from", "~to", "~label", "w:Double"])
        for j in range(n_edges):
            a = rnd.randrange(n_vertices)
            b = rnd.randrange(n_vertices)
            w.writerow([f"e{chunk}_{j}", f"v{chunk}_{a}", f"v{chunk}_{b}",
                        "LINK", round(rnd.random(), 6)])
    return vpath, epath


def wait_load(client, load_id, poll=30):
    while True:
        resp = client.get_loader_job_status(loadId=load_id)
        overall = resp.get("payload", {}).get("overallStatus", {})
        status = overall.get("status")
        log(f"[load] {load_id} status={status} "
            f"records={overall.get('totalRecords')} "
            f"time={overall.get('totalTimeSpent')}s")
        if status in ("LOAD_COMPLETED",):
            return overall
        if status and (status.startswith("LOAD_FAILED") or status in (
                "LOAD_CANCELLED_BY_USER", "LOAD_CANCELLED_DUE_TO_ERRORS",
                "LOAD_DATA_FAILED_DUE_TO_FEED_MODIFIED_OR_DELETED",
                "LOAD_S3_READ_ERROR", "LOAD_S3_ACCESS_DENIED_ERROR",
                "LOAD_COMMITTED_W_WRITE_CONFLICTS", "LOAD_DATA_DEADLOCK",
                "LOAD_FAILED_INVALID_REQUEST")):
            raise RuntimeError(f"Bulk load {load_id} ended with {status}: "
                               f"{json.dumps(resp, default=str)[:2000]}")
        time.sleep(poll)


def cmd_load(a):
    s3 = boto3.client("s3", region_name=a.region)
    client = ndata(a.endpoint, a.region)
    os.makedirs(a.workdir, exist_ok=True)
    info = get_storage_info(a.endpoint, a.region)
    log(f"[load] starting totalStorageBytesUsed="
        f"{info.get('totalStorageBytesUsed', 0) / GIB:.3f} GiB")

    for chunk in range(a.start_chunk, a.start_chunk + a.max_chunks):
        log(f"[load] chunk {chunk}: generating {a.vertices} vertices, {a.edges} edges")
        vpath, epath = write_chunk(a.workdir, chunk, a.vertices, a.edges, seed=chunk)
        prefix = f"load/chunk_{chunk:03d}/"
        for p in (vpath, epath):
            s3.upload_file(p, a.bucket, prefix + os.path.basename(p))
            os.remove(p)
        log(f"[load] chunk {chunk}: uploaded to s3://{a.bucket}/{prefix}")

        resp = client.start_loader_job(
            source=f"s3://{a.bucket}/{prefix}",
            format="csv",
            s3BucketRegion=a.region,
            iamRoleArn=a.loader_role,
            failOnError=True,
            parallelism="OVERSUBSCRIBE",
        )
        load_id = resp["payload"]["loadId"]
        log(f"[load] chunk {chunk}: loadId={load_id}")
        wait_load(client, load_id)

        info = get_storage_info(a.endpoint, a.region)
        total = info.get("totalStorageBytesUsed", 0)
        log(f"[load] after chunk {chunk}: totalStorageBytesUsed={total / GIB:.3f} GiB")
        if total >= a.target_gib * GIB:
            log(f"[load] reached target {a.target_gib} GiB; stopping")
            break
    log("[load] DONE")


# ----------------------------------------------------------------- delete
DELETE_Q = ("MATCH (n:Item) WHERE n.bucket = $b WITH n LIMIT $lim "
            "DETACH DELETE n RETURN count(*) AS c")


def cmd_delete(a):
    client = ndata(a.endpoint, a.region)
    lock = threading.Lock()
    totals = {"vertices": 0, "batches": 0}

    def run_bucket(b):
        while True:
            resp = with_backoff(lambda: client.execute_open_cypher_query(
                openCypherQuery=DELETE_Q,
                parameters=json.dumps({"b": b, "lim": a.batch})))
            c = resp["results"][0]["c"]
            with lock:
                totals["vertices"] += c
                totals["batches"] += 1
                if totals["batches"] % 25 == 0:
                    log(f"[delete] batches={totals['batches']} "
                        f"vertices_deleted={totals['vertices']}")
            if c == 0:
                log(f"[delete] bucket {b} empty")
                return

    log(f"[delete] deleting buckets {a.buckets} batch={a.batch} workers={a.workers}")
    with ThreadPoolExecutor(max_workers=a.workers) as ex:
        list(ex.map(run_bucket, a.buckets))
    log(f"[delete] DONE vertices_deleted={totals['vertices']} batches={totals['batches']}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--endpoint", required=True)
    p.add_argument("--region", required=True)
    sub = p.add_subparsers(dest="cmd", required=True)

    l = sub.add_parser("load")
    l.add_argument("--bucket", required=True)
    l.add_argument("--loader-role", required=True)
    l.add_argument("--workdir", default="/opt/run/data")
    l.add_argument("--vertices", type=int, default=1_000_000)
    l.add_argument("--edges", type=int, default=4_000_000)
    l.add_argument("--target-gib", type=float, default=3.5)
    l.add_argument("--start-chunk", type=int, default=0)
    l.add_argument("--max-chunks", type=int, default=8)

    d = sub.add_parser("delete")
    d.add_argument("--buckets", type=int, nargs="+", default=[0, 1, 2, 3, 4, 5])
    d.add_argument("--batch", type=int, default=2000)
    d.add_argument("--workers", type=int, default=2)

    a = p.parse_args()
    {"load": cmd_load, "delete": cmd_delete}[a.cmd](a)


if __name__ == "__main__":
    main()
