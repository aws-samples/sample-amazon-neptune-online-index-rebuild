"""
preflight.py - Check that it is safe and useful to start an online index rebuild.

Checks: engine version, writer is provisioned (not Serverless) and the endpoint
is the writer, no rebuild already running, no bulk load running, streams setting
and expiry (streams skew before/after measurements), dictionary GC mode, and
IAM access to the data-plane APIs. Exit code 1 if a blocking check fails.
"""

import argparse
import sys

import boto3
from botocore.exceptions import ClientError

from index_rebuild import active_rebuild, neptune_client

MIN_ENGINE = (1, 5, 0, 0)
RECOMMENDED_ENGINE = (1, 5, 0, 1)


def _ver(s):
    return tuple(int(x) for x in s.split(".")[:4])


def _params(rds, group):
    out, marker = {}, None
    while True:
        kw = {"DBClusterParameterGroupName": group}
        if marker:
            kw["Marker"] = marker
        resp = rds.describe_db_cluster_parameters(**kw)
        for p in resp.get("Parameters", []):
            out[p["ParameterName"]] = p.get("ParameterValue")
        marker = resp.get("Marker")
        if not marker:
            return out


def run(endpoint, region, cluster_id, port=8182):
    ok, warn, fail = [], [], []
    rds = boto3.client("neptune", region_name=region)
    cluster = rds.describe_db_clusters(DBClusterIdentifier=cluster_id)["DBClusters"][0]

    v = cluster["EngineVersion"]
    if _ver(v) < MIN_ENGINE:
        fail.append(f"engine {v} is older than 1.5.0.0")
    elif _ver(v) < RECOMMENDED_ENGINE:
        warn.append(f"engine {v}: 1.5.0.1 or later is recommended")
    else:
        ok.append(f"engine {v}")

    writer = next((m for m in cluster["DBClusterMembers"] if m["IsClusterWriter"]), None)
    if writer:
        inst = rds.describe_db_instances(
            DBInstanceIdentifier=writer["DBInstanceIdentifier"])["DBInstances"][0]
        if inst["DBInstanceClass"] == "db.serverless":
            fail.append("writer is Neptune Serverless; online index rebuild needs a provisioned writer")
        else:
            ok.append(f"writer {inst['DBInstanceIdentifier']} ({inst['DBInstanceClass']})")
        writer_hosts = {cluster["Endpoint"], inst["Endpoint"]["Address"]}
        if endpoint not in writer_hosts:
            warn.append(f"{endpoint} is not the cluster or writer endpoint; requests must reach the writer")

    params = _params(rds, cluster["DBClusterParameterGroup"])
    streams = (params.get("neptune_streams") or "0") == "1"
    expiry = params.get("neptune_streams_expiry_days") or "7"
    if streams:
        warn.append(f"streams enabled (expiry {expiry} days): deletes add stream records; "
                    "measure final results after they expire")
    else:
        ok.append("streams disabled")
    lab = params.get("neptune_lab_mode") or ""
    gc = next((p.split("=", 1)[1] for p in lab.split(",") if p.startswith("DictionaryGCMode=")),
              "disabled")
    (ok if gc != "disabled" else warn).append(
        f"dictionary GC mode: {gc}" + ("" if gc != "disabled"
                                       else " (dictionary bloat will not be cleaned up)"))

    client = neptune_client(endpoint, region, port)
    try:
        found = active_rebuild(client)
        (fail if found else ok).append(
            f"rebuild already running: {found[0]} ({found[1].get('status')}, "
            f"scope {found[1].get('scope')}, {found[1].get('progressPercentage')}%)" if found
            else "no rebuild running")
    except ClientError as err:
        fail.append(f"ListIndexRebuilds failed: {err.response['Error']['Code']}")
    try:
        client.get_storage_info()
        ok.append("GetStorageInfo allowed")
    except ClientError as err:
        fail.append(f"GetStorageInfo failed: {err.response['Error']['Code']}")
    try:
        loads = client.list_loader_jobs(includeQueuedLoads=True)["payload"].get("loadIds", [])
        running = []
        for lid in loads[:10]:
            st = client.get_loader_job_status(loadId=lid)["payload"]["overallStatus"]["status"]
            if st in ("LOAD_IN_PROGRESS", "LOAD_NOT_STARTED", "LOAD_IN_QUEUE"):
                running.append(lid)
        (fail if running else ok).append(
            f"bulk load active: {running}; loads are rejected during a rebuild" if running
            else "no bulk load running")
    except ClientError as err:
        warn.append(f"could not list loader jobs: {err.response['Error']['Code']}")
    return ok, warn, fail


def main():
    ap = argparse.ArgumentParser(description="Preflight checks for an online index rebuild.")
    ap.add_argument("--endpoint", required=True, help="writer or cluster endpoint")
    ap.add_argument("--region", required=True)
    ap.add_argument("--cluster-id", required=True)
    ap.add_argument("--port", type=int, default=8182)
    a = ap.parse_args()
    ok, warn, fail = run(a.endpoint, a.region, a.cluster_id, a.port)
    for tag, items in (("OK  ", ok), ("WARN", warn), ("FAIL", fail)):
        for i in items:
            print(f"[{tag}] {i}")
    sys.exit(1 if fail else 0)


if __name__ == "__main__":
    main()
