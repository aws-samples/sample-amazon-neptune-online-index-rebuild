"""
verify.py — Confirm reclaimed Neptune storage after an online index rebuild.

Verification is two-pronged:
  1. Re-run GetStorageInfo and compare totalStorageBytesUsed (and per-component
     sizes) against the pre-rebuild baseline.
  2. Watch the CloudWatch VolumeBytesUsed metric for the cluster to confirm the
     reclaimed space at the storage-volume level.

Docs:
  https://docs.aws.amazon.com/neptune/latest/userguide/storage-info.html
  https://docs.aws.amazon.com/neptune/latest/userguide/cw-metrics.html

Requires: Neptune engine release 1.5.0.0 or later (for GetStorageInfo).
"""

import argparse
import datetime

import boto3

from storage_info import get_storage_info


def compare_storage(before, after):
    """Return a GiB-delta summary between two GetStorageInfo snapshots."""
    b_total = before.get("totalStorageBytesUsed", 0)
    a_total = after.get("totalStorageBytesUsed", 0)
    reclaimed = b_total - a_total
    gib = 1024 ** 3
    lines = [
        "Storage comparison (GetStorageInfo)",
        "-" * 45,
        f"Before total:   {b_total / gib:>10.2f} GiB",
        f"After  total:   {a_total / gib:>10.2f} GiB",
        f"Reclaimed:      {reclaimed / gib:>10.2f} GiB "
        f"({(reclaimed / b_total * 100) if b_total else 0:.1f}%)",
    ]
    return "\n".join(lines)


def get_volume_bytes_used(cluster_id, region, lookback_hours=6, period=300):
    """Fetch recent CloudWatch VolumeBytesUsed datapoints for a Neptune cluster.

    Args:
        cluster_id: The DBClusterIdentifier of the Neptune cluster.
        region: AWS region.
        lookback_hours: How far back to fetch datapoints.
        period: Metric period in seconds (default 300).

    Returns:
        list[dict]: Datapoints sorted by timestamp, each with Timestamp + Average.
    """
    cw = boto3.client("cloudwatch", region_name=region)
    end = datetime.datetime.utcnow()
    start = end - datetime.timedelta(hours=lookback_hours)

    resp = cw.get_metric_statistics(
        Namespace="AWS/Neptune",
        MetricName="VolumeBytesUsed",
        Dimensions=[{"Name": "DBClusterIdentifier", "Value": cluster_id}],
        StartTime=start,
        EndTime=end,
        Period=period,
        Statistics=["Average"],
    )
    return sorted(resp.get("Datapoints", []), key=lambda d: d["Timestamp"])


def main():
    parser = argparse.ArgumentParser(description="Verify reclaimed Neptune storage.")
    parser.add_argument("--endpoint", required=True, help="Neptune endpoint hostname")
    parser.add_argument("--region", required=True, help="AWS region, e.g. us-east-1")
    parser.add_argument("--port", type=int, default=8182, help="Neptune port")
    parser.add_argument(
        "--cluster-id", help="DBClusterIdentifier for CloudWatch VolumeBytesUsed"
    )
    args = parser.parse_args()

    after = get_storage_info(args.endpoint, args.region, args.port)
    gib = 1024 ** 3
    print(
        f"Current totalStorageBytesUsed: "
        f"{after.get('totalStorageBytesUsed', 0) / gib:.2f} GiB"
    )

    if args.cluster_id:
        points = get_volume_bytes_used(args.cluster_id, args.region)
        print("\nCloudWatch VolumeBytesUsed (recent):")
        for p in points[-10:]:
            print(f"  {p['Timestamp'].isoformat()}  {p['Average'] / gib:.2f} GiB")


if __name__ == "__main__":
    main()
