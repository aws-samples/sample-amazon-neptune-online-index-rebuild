"""
storage_info.py — Diagnose Amazon Neptune storage usage with the GetStorageInfo API.

The GetStorageInfo API (Neptune engine release 1.5.0.0+) returns a per-component
breakdown of on-disk storage: quad indexes, dictionaries, undo logs, streams, and
more. Use it to see exactly where storage is going, identify storage bloat, and
establish a baseline before and after an online index rebuild.

Docs:
  https://docs.aws.amazon.com/neptune/latest/userguide/storage-info.html

Requires: Neptune engine release 1.5.0.0 or later.
"""

import argparse
import json

import boto3


def get_storage_info(endpoint, region, port=8182):
    """Call GetStorageInfo and return the parsed response.

    Args:
        endpoint: Neptune cluster/instance endpoint hostname (no scheme, no port).
        region: AWS region of the cluster (e.g. "us-east-1").
        port: Neptune port (default 8182).

    Returns:
        dict: The GetStorageInfo response, including per-component byte counts and
              a totalStorageBytesUsed value that sums the reported on-disk components.
    """
    client = boto3.client(
        "neptunedata",
        endpoint_url=f"https://{endpoint}:{port}",
        region_name=region,
    )
    return client.get_storage_info()


def summarize(storage_info):
    """Return a human-readable per-component storage summary.

    Args:
        storage_info: The dict returned by get_storage_info().

    Returns:
        str: A formatted table of components and their sizes in GiB.
    """
    lines = ["Component                         Size (GiB)", "-" * 45]
    components = storage_info.get("storageBreakdown", storage_info)
    total = storage_info.get("totalStorageBytesUsed")

    if isinstance(components, dict):
        for name, value in sorted(components.items()):
            if isinstance(value, (int, float)):
                lines.append(f"{name:<32} {value / (1024 ** 3):>10.2f}")

    if total is not None:
        lines.append("-" * 45)
        lines.append(f"{'totalStorageBytesUsed':<32} {total / (1024 ** 3):>10.2f}")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Diagnose Neptune storage usage.")
    parser.add_argument("--endpoint", required=True, help="Neptune endpoint hostname")
    parser.add_argument("--region", required=True, help="AWS region, e.g. us-east-1")
    parser.add_argument("--port", type=int, default=8182, help="Neptune port (default 8182)")
    parser.add_argument("--raw", action="store_true", help="Print the raw JSON response")
    args = parser.parse_args()

    info = get_storage_info(args.endpoint, args.region, args.port)
    if args.raw:
        print(json.dumps(info, indent=2, default=str))
    else:
        print(summarize(info))


if __name__ == "__main__":
    main()
