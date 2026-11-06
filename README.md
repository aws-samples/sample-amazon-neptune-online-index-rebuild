# Reclaim Amazon Neptune storage with online index rebuild

Companion code for the AWS Database Blog post **"Reclaim Amazon Neptune storage
with zero downtime using online index rebuild."**

Amazon Neptune engine release **1.5.0.0** (October 13, 2026) introduced two new
storage-management APIs:

- **GetStorageInfo** — a per-component breakdown of on-disk storage (quad
  indexes, dictionaries, undo logs, streams, and more), plus a
  `totalStorageBytesUsed` roll-up you can use as a baseline.
- **Online index rebuild** — defragments quad and/or dictionary indexes and
  reclaims unused storage space **while the database stays fully operational**.
  Concurrent reads and writes continue across Gremlin, openCypher, and SPARQL.

This repo gives you a runnable **boto3** and **AWS CLI** implementation of the
end-to-end workflow in the post: **diagnose → reclaim → monitor → verify.**

## Why this matters

Over time, a Neptune graph accumulates index fragmentation from ongoing inserts,
updates, and deletes. The painful symptom: the `VolumeBytesUsed` metric stays
high even after you delete large amounts of data, so you keep paying for storage
you think you freed — and fragmentation spreads the same logical data across more
physical pages, inflating I/O cost and hurting query performance. Online index
rebuild reclaims that space in place, with no downtime.

## Prerequisites

- A Neptune cluster on **engine release 1.5.0.0 or later**.
- Python 3.9+ and `pip install -r requirements.txt` (boto3), or AWS CLI v2.
- Network access to the Neptune endpoint — Neptune is VPC-only, so run from
  inside the VPC (for example an EC2 bastion or a VPC-attached environment).
- Credentials allowing the `neptunedata` actions used below. If IAM database
  authentication is enabled on the cluster, your principal needs the
  corresponding `neptune-db:*` permissions (for example
  `neptune-db:StartIndexRebuild`, `neptune-db:GetIndexRebuildStatus`,
  `neptune-db:CancelIndexRebuild`), plus `cloudwatch:GetMetricStatistics` for
  verification.

## Repository layout

```
amazon-neptune-online-index-rebuild/
├── src/
│   ├── storage_info.py     # GetStorageInfo baseline + per-component breakdown
│   ├── index_rebuild.py    # Start/scope rebuild, poll status, retry handling
│   └── verify.py           # Re-run GetStorageInfo + CloudWatch VolumeBytesUsed
├── cli/
│   └── commands.sh         # Equivalent AWS CLI invocations
├── examples/
│   └── end_to_end_walkthrough.py   # diagnose → rebuild → monitor → verify
└── requirements.txt
```

## Quickstart (boto3)

Install dependencies and run the end-to-end walkthrough:

```bash
pip install -r requirements.txt

python examples/end_to_end_walkthrough.py \
  --endpoint my-cluster.cluster-abc123.us-east-1.neptune.amazonaws.com \
  --region us-east-1 \
  --cluster-id my-neptune-cluster \
  --target QUADS
```

Or run each step on its own:

```bash
# 1. Diagnose — establish a baseline
python src/storage_info.py --endpoint <endpoint> --region us-east-1

# 2. Reclaim — start a scoped rebuild and wait for it to finish
python src/index_rebuild.py --endpoint <endpoint> --region us-east-1 \
  --target QUADS --wait

# 3. Verify — compare storage and inspect CloudWatch VolumeBytesUsed
python src/verify.py --endpoint <endpoint> --region us-east-1 \
  --cluster-id my-neptune-cluster
```

## Quickstart (AWS CLI)

The same flow with the `neptunedata` CLI command family:

```bash
export NEPTUNE_ENDPOINT=my-cluster.cluster-abc123.us-east-1.neptune.amazonaws.com
export AWS_REGION=us-east-1
export NEPTUNE_CLUSTER_ID=my-neptune-cluster
export REBUILD_TARGET=QUADS

bash cli/commands.sh
```

## Scoping and tuning

- **`--target`** — scope the rebuild to `ALL`, `QUADS`, or `DICTIONARIES`. Start
  narrow (for example `QUADS`) if that is where your fragmentation is.
- **`--concurrency`** — higher parallelism finishes faster but uses more cluster
  resources; tune for your workload and headroom.
- **`--write-buffer-size`** — buffer size (MiB) used during the rebuild.

## Handling `CancelledByMaintenanceException`

During a rebuild the storage layer can briefly enter maintenance mode, which
cancels an in-flight request with **`CancelledByMaintenanceException`**. This is
**retriable** — treat it like `ConcurrentModificationException`. Every Neptune
call in `src/index_rebuild.py` is wrapped in `with_backoff()`, which retries only
on these two errors using exponential backoff with full jitter, so your
application stays resilient while a rebuild runs.

## Verifying reclaimed space

1. Re-run **GetStorageInfo** and compare `totalStorageBytesUsed` (and the
   per-component sizes) against your pre-rebuild baseline.
2. Watch the CloudWatch **`VolumeBytesUsed`** metric (namespace `AWS/Neptune`,
   dimension `DBClusterIdentifier`) to confirm the reclaimed space at the
   storage-volume level.

## References

- [Neptune engine release 1.5.0.0](https://docs.aws.amazon.com/neptune/latest/userguide/engine-releases-1.5.0.0.html)
- [Neptune storage usage breakdown (GetStorageInfo)](https://docs.aws.amazon.com/neptune/latest/userguide/storage-info.html)
- [Neptune online index rebuild](https://docs.aws.amazon.com/neptune/latest/userguide/storage-index-rebuild.html)
- [Neptune storage cleanup](https://docs.aws.amazon.com/neptune/latest/userguide/storage-cleanup.html)

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more
information.

## License

This library is licensed under the MIT-0 License. See the [LICENSE](LICENSE) file.
