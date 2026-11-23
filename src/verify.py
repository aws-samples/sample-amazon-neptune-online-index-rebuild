"""
verify.py - Compare storage before and after a rebuild, and track VolumeBytesUsed.

A rebuild writes a new copy of each index and drops the old one, so reclaimed
space should come off the cluster volume and show up as a drop in the CloudWatch
VolumeBytesUsed metric (the billed figure). That metric is published with a delay,
and it can rise first because a rebuild needs scratch space (up to ~25% of the
data being rebuilt). Watch it for 24-72 hours after the rebuild.

GetStorageInfo counts are approximate; use it for the per-component picture and
VolumeBytesUsed for billing.
"""

import argparse
import datetime as dt
import json

import boto3

from storage_info import components, get_storage_info

GIB = 1024 ** 3
MIB = 1024 ** 2


def compare_storage(before, after):
    """Per-component and total comparison of two GetStorageInfo snapshots."""
    b, a = components(before), components(after)
    lines = [f"{'Component':<24}{'Before MiB':>12}{'After MiB':>12}{'Change':>10}", "-" * 58]
    for key in sorted(set(b) | set(a)):
        x, y = b.get(key, 0), a.get(key, 0)
        pct = f"{(y - x) / x * 100:+.1f}%" if x else "new"
        lines.append(f"{key:<24}{x / MIB:>12.1f}{y / MIB:>12.1f}{pct:>10}")
    bt, at = before.get("totalStorageBytesUsed", 0), after.get("totalStorageBytesUsed", 0)
    lines += ["-" * 58,
              f"{'totalStorageBytesUsed':<24}{bt / GIB:>11.2f}G{at / GIB:>11.2f}G"
              f"{((at - bt) / bt * 100) if bt else 0:>+9.1f}%"]
    return "\n".join(lines)


def get_volume_bytes_used(cluster_id, region, start=None, end=None,
                          lookback_hours=72, period=3600):
    cw = boto3.client("cloudwatch", region_name=region)
    end = end or dt.datetime.now(dt.timezone.utc)
    start = start or end - dt.timedelta(hours=lookback_hours)
    resp = cw.get_metric_statistics(
        Namespace="AWS/Neptune", MetricName="VolumeBytesUsed",
        Dimensions=[{"Name": "DBClusterIdentifier", "Value": cluster_id}],
        StartTime=start, EndTime=end, Period=period, Statistics=["Average"])
    return sorted(resp.get("Datapoints", []), key=lambda d: d["Timestamp"])


def main():
    ap = argparse.ArgumentParser(description="Verify reclaimed Neptune storage.")
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--region", required=True)
    ap.add_argument("--port", type=int, default=8182)
    ap.add_argument("--baseline", help="JSON saved earlier with storage_info.py --save")
    ap.add_argument("--cluster-id", help="DBClusterIdentifier for VolumeBytesUsed")
    ap.add_argument("--lookback-hours", type=int, default=72)
    a = ap.parse_args()

    after = get_storage_info(a.endpoint, a.region, a.port)
    if a.baseline:
        with open(a.baseline) as f:
            print(compare_storage(json.load(f), after))
    else:
        print(f"totalStorageBytesUsed: {after.get('totalStorageBytesUsed', 0) / GIB:.2f} GiB "
              "(pass --baseline to compare)")
    if a.cluster_id:
        print(f"\nVolumeBytesUsed, last {a.lookback_hours} h (hourly averages):")
        for p in get_volume_bytes_used(a.cluster_id, a.region, lookback_hours=a.lookback_hours):
            print(f"  {p['Timestamp'].isoformat()}  {p['Average'] / GIB:.3f} GiB")


if __name__ == "__main__":
    main()
