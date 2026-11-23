"""
end_to_end_walkthrough.py - preflight -> diagnose -> rebuild -> monitor -> verify.

Requires Neptune engine 1.5.0.0 or later (1.5.0.1 recommended), a provisioned
writer, and Python 3.11+. Use the writer (or cluster) endpoint.

  python examples/end_to_end_walkthrough.py --endpoint <writer-endpoint> \
      --region us-east-1 --cluster-id my-neptune-cluster --scope QUADS
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from index_rebuild import start_index_rebuild, wait_for_completion  # noqa: E402
from preflight import run as preflight  # noqa: E402
from storage_info import get_storage_info, summarize  # noqa: E402
from verify import compare_storage  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description="Neptune online index rebuild walkthrough.")
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--region", required=True)
    ap.add_argument("--cluster-id", required=True)
    ap.add_argument("--port", type=int, default=8182)
    ap.add_argument("--scope", default="ALL", choices=("ALL", "QUADS", "DICTIONARIES"))
    ap.add_argument("--concurrency", type=int)
    ap.add_argument("--write-buffer-size", choices=("LOW", "MEDIUM", "HIGH"))
    ap.add_argument("--poll-seconds", type=int, default=30)
    ap.add_argument("--baseline-out", default="baseline.json")
    ap.add_argument("--skip-preflight", action="store_true")
    a = ap.parse_args()

    if not a.skip_preflight:
        print("=== Preflight ===")
        ok, warn, fail = preflight(a.endpoint, a.region, a.cluster_id, a.port)
        for tag, items in (("OK  ", ok), ("WARN", warn), ("FAIL", fail)):
            for i in items:
                print(f"[{tag}] {i}")
        if fail:
            sys.exit("Preflight failed; fix the FAIL items before starting a rebuild.")

    print("\n=== Step 1: Diagnose ===")
    before = get_storage_info(a.endpoint, a.region, a.port)
    with open(a.baseline_out, "w") as f:
        json.dump(before, f, indent=2, default=str)
    print(summarize(before))
    print(f"(baseline saved to {a.baseline_out})")

    print(f"\n=== Step 2: Reclaim (scope={a.scope}) ===")
    rid = start_index_rebuild(a.endpoint, a.region, a.scope, a.concurrency,
                              a.write_buffer_size, a.port)
    print(f"rebuildId={rid}")

    print("\n=== Step 3: Monitor ===")
    final = wait_for_completion(a.endpoint, a.region, rid, a.poll_seconds, a.port)
    if (final.get("status") or "").upper() != "COMPLETED":
        sys.exit(f"Rebuild ended with {final.get('status')}; skipping verification.")

    print("\n=== Step 4: Verify ===")
    after = get_storage_info(a.endpoint, a.region, a.port)
    print(compare_storage(before, after))
    print(f"\nNext: track VolumeBytesUsed for 24-72 hours:\n  python src/verify.py "
          f"--endpoint {a.endpoint} --region {a.region} --cluster-id {a.cluster_id} "
          f"--baseline {a.baseline_out}")


if __name__ == "__main__":
    main()
