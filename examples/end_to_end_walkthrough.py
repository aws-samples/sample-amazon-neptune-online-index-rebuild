"""
end_to_end_walkthrough.py — Diagnose -> rebuild -> monitor -> verify, end to end.

This mirrors the workflow in the blog post:
  1. Diagnose with GetStorageInfo (establish a baseline).
  2. Reclaim with an online index rebuild (scoped, with retry handling).
  3. Monitor by polling rebuild status until it completes.
  4. Verify by re-running GetStorageInfo and comparing totals.

Run against a Neptune cluster on engine release 1.5.0.0 or later.

Example:
  python examples/end_to_end_walkthrough.py \
      --endpoint my-cluster.cluster-abc123.us-east-1.neptune.amazonaws.com \
      --region us-east-1 \
      --cluster-id my-neptune-cluster \
      --target QUADS
"""

import argparse
import os
import sys

# Make src/ importable when run from the repo root.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from storage_info import get_storage_info, summarize          # noqa: E402
from index_rebuild import start_index_rebuild, wait_for_completion  # noqa: E402
from verify import compare_storage                            # noqa: E402


def main():
    parser = argparse.ArgumentParser(description="Neptune storage reclaim walkthrough.")
    parser.add_argument("--endpoint", required=True, help="Neptune endpoint hostname")
    parser.add_argument("--region", required=True, help="AWS region, e.g. us-east-1")
    parser.add_argument("--port", type=int, default=8182, help="Neptune port")
    parser.add_argument("--cluster-id", help="DBClusterIdentifier (for your records)")
    parser.add_argument(
        "--target", default="ALL", choices=("ALL", "QUADS", "DICTIONARIES")
    )
    parser.add_argument("--concurrency", type=int, default=None)
    parser.add_argument("--write-buffer-size", type=int, default=None)
    parser.add_argument("--poll-seconds", type=int, default=30)
    args = parser.parse_args()

    # 1. Diagnose — baseline.
    print("=== Step 1: Diagnose (baseline) ===")
    before = get_storage_info(args.endpoint, args.region, args.port)
    print(summarize(before))

    # 2. Reclaim — start the online index rebuild.
    print(f"\n=== Step 2: Reclaim (online index rebuild, target={args.target}) ===")
    op_id = start_index_rebuild(
        args.endpoint,
        args.region,
        target=args.target,
        concurrency=args.concurrency,
        write_buffer_size=args.write_buffer_size,
        port=args.port,
    )
    print(f"Started rebuild; operationId={op_id}")

    # 3. Monitor — poll to completion.
    print("\n=== Step 3: Monitor ===")
    final = wait_for_completion(
        args.endpoint, args.region, op_id, args.poll_seconds, args.port
    )
    state = (final.get("status") or final.get("state") or "UNKNOWN").upper()
    print(f"Rebuild finished with state: {state}")
    if state != "COMPLETED":
        print("Rebuild did not complete successfully; skipping verification.")
        sys.exit(1)

    # 4. Verify — compare storage.
    print("\n=== Step 4: Verify ===")
    after = get_storage_info(args.endpoint, args.region, args.port)
    print(compare_storage(before, after))
    print(
        "\nTip: also watch the CloudWatch VolumeBytesUsed metric for cluster "
        f"'{args.cluster_id or '<your-cluster-id>'}' to confirm at the volume level."
    )


if __name__ == "__main__":
    main()
