# Keep Amazon Neptune storage healthy with online index rebuild

Companion code for the AWS Database Blog post on Neptune storage cleanup with
GetStorageInfo, dictionary garbage collection, and online index rebuild
(title to be finalized).

Neptune engine release 1.5.0.0 added two storage-management APIs:

- **GetStorageInfo** - a per-component breakdown of storage (quad indexes,
  dictionaries, undo logs, streams, and more). Counts are approximate; use the
  CloudWatch `VolumeBytesUsed` metric for billing.
- **Online index rebuild** - compacts quad and/or dictionary indexes while the
  database stays online. A `QUADS` or `ALL` rebuild covers every quad index,
  including OSGP and the PZSOG geospatial index.

Storage that stays high after deletes usually has two sources: dead **dictionary**
entries (Neptune keeps distinct dictionary entries until dictionary garbage
collection removes them) and **fragmented quad indexes**. This repo helps you
check both, then compact and verify.

## Prerequisites

- Neptune engine **1.5.0.0 or later; 1.5.0.1 recommended**.
- A **provisioned writer**. Online index rebuild is not supported on a Neptune
  Serverless writer. Send requests to the writer (or cluster) endpoint.
- **Python 3.11 or later** (tested on 3.12) and `pip install -r requirements.txt`,
  or AWS CLI v2.
- Network access to the endpoint from inside the VPC.
- With IAM database auth: `neptune-db:GetStorageInfo`, `neptune-db:StartIndexRebuild`,
  `neptune-db:GetIndexRebuildStatus`, `neptune-db:ListIndexRebuilds`,
  `neptune-db:CancelIndexRebuild`, and `neptune-db:GetGraphSummary`; for the
  preflight, `neptune-db:ListLoaderJobs`, `neptune-db:GetLoaderJobStatus`,
  `rds:DescribeDBClusters`, `rds:DescribeDBInstances`, and
  `rds:DescribeDBClusterParameters`; for verification,
  `cloudwatch:GetMetricStatistics`. The optional `--refresh-stats` flag of
  `storage_info.py` also needs `neptune-db:ManageStatistics` and
  `neptune-db:GetStatisticsStatus`.

## Repository layout

```
src/                 reusable code
  preflight.py       safety checks before a rebuild
  storage_info.py    GetStorageInfo summary, bytes-per-quad and dictionary checks
  index_rebuild.py   start / monitor / cancel a rebuild
  query_retry.py     retry pattern for YOUR application queries during a rebuild
  verify.py          before/after comparison and VolumeBytesUsed tracking
examples/end_to_end_walkthrough.py
cli/commands.sh      AWS CLI version of the workflow
tests/               offline tests (pytest)
infra/               CloudFormation test stack used for the blog run (creates billable resources)
scripts/             test harness used for the blog's measurements - NOT reusable code
  stage_data_v2.py   generates and loads a test graph, then updates/deletes part of it
  benchmark_queries.py  fixed query workload with p50/p95/p99 latency
  probe.py           RETURN 1 availability probe during a rebuild
  run_reclaim.py     instrumented rebuild with a concurrent workload
  teardown.py        empties the test stack's buckets and deletes the stack
```

## Quickstart (boto3)

```bash
pip install -r requirements.txt

# 0. Preflight
python src/preflight.py --endpoint <writer-endpoint> --region us-east-1 --cluster-id my-cluster

# 1. Diagnose and save a baseline. --dictionary-check adds the dictionary-entries-per-
#    live-element ratio; --refresh-stats refreshes statistics first (the live count
#    comes from statistics that may be stale); --save-health writes bytes-per-quad
#    and the dictionary check to JSON.
python src/storage_info.py --endpoint <writer-endpoint> --region us-east-1 \
  --save baseline.json --dictionary-check --refresh-stats --save-health baseline-health.json

# 2-3. Rebuild and wait
python src/index_rebuild.py --endpoint <writer-endpoint> --region us-east-1 --scope QUADS --wait

# 4. Verify now, then again over the next 24-72 hours
python src/verify.py --endpoint <writer-endpoint> --region us-east-1 \
  --cluster-id my-cluster --baseline baseline.json
```

Or run `examples/end_to_end_walkthrough.py`, which chains all of these.

## Quickstart (AWS CLI)

```bash
export NEPTUNE_ENDPOINT=<writer-endpoint>
export AWS_REGION=us-east-1
export NEPTUNE_CLUSTER_ID=my-cluster
export REBUILD_SCOPE=QUADS     # ALL | QUADS | DICTIONARIES (default ALL)
bash cli/commands.sh
```

## Reading the diagnosis

- **Bytes per quad** (SPOG/POGS/GPSO/OSGP, indexes over ~1M quads): about 50-60 is
  compact, 60-90 normal with ongoing writes, 90-150 noticeably fragmented, over 150
  heavily fragmented. Reclaimable space is roughly `sizeInBytes - count x 52`. These
  bands come from one property graph test and are estimates. PZSOG is reported but
  not banded.
- **Dictionary**: the tool reports `dictionaries.total.count` per live vertex + edge
  as a **ratio**, not a dead-entry percentage. Each element can need several
  dictionary entries (ids, string values), so a ratio above 1 is normal. What
  matters is the **trend**: entries growing while live elements stay flat suggests
  entries that dictionary garbage collection could clean up. If you see that,
  enable dictionary garbage collection (`neptune_lab_mode`
  `DictionaryGCMode=soft_delete`, later `enabled`; needs a writer reboot; property
  graph only; pauses when freeable memory is under 15 GB, during bulk loads, or
  when UndoLogListSize is over 1,000,000). There is no dictionary cleanup for RDF
  data.
- **Counts are approximate, and statistics may need a refresh.** GetStorageInfo
  counts come from periodically refreshed row-count statistics and can drift,
  especially after large loads or deletes. The live-element count comes from the
  graph summary API, which uses Neptune statistics that can be stale. Use
  `--refresh-stats` (or refresh statistics yourself) before comparing, and check the
  "Statistics last computed" line in the output.

## During a rebuild

- **Bulk loads and fast resets are rejected** (`CancelledByMaintenanceException`)
  until the rebuild finishes or is cancelled. Don't retry them with backoff; wait
  or cancel first.
- `writeBufferSize` (`LOW`/`MEDIUM`/`HIGH`) sizes the buffer for concurrent
  **query** writes. Raise it under heavy write traffic.
- Your **application queries** can fail with `CancelledByMaintenanceException`.
  Retry them with backoff; see `src/query_retry.py` for openCypher, Gremlin, Bolt,
  and SPARQL examples.
- Only one rebuild runs at a time. Its record is lost on restart or failover.
  Cancelling can take hours. Read replicas are not affected.
- `VolumeBytesUsed` can rise during the rebuild (scratch space, up to ~25% of the
  rebuilt data) before it falls.
- If streams are enabled, a large delete adds stream records that stay for
  `neptune_streams_expiry_days` (default 7). The rebuild does not touch them.

## Test stack (`infra/`)

`infra/neptune-blog-stack.yaml` creates a VPC, a Neptune cluster, an EC2 runner,
and S3 buckets. It is for reproducing the blog run, not for production, and it
costs money while it exists.

Deploy it once per cluster. Names default from the stack name (use a lowercase stack
name), so two stacks can run side by side with different parameters:

| Parameter | Main cluster (A) | Streams cluster (B) |
|---|---|---|
| `NeptuneInstanceClass` | `db.r6g.2xlarge` | `db.r6g.large` |
| `LabMode` (`neptune_lab_mode`) | `DictionaryGCMode=soft_delete` | empty (not set) |
| `StreamsEnabled` (`neptune_streams`) | `0` | `1` |
| `StreamsExpiryDays` | `7` (unused) | `1` |

`ParameterGroupFamily` has no default: look it up for your engine version with
`aws neptune describe-db-engine-versions --engine neptune --engine-version <version>
--query "DBEngineVersions[].DBParameterGroupFamily"`.

`neptune_lab_mode` and `neptune_streams*` are **static** cluster parameters. A new
cluster picks them up at creation; after changing them on a running cluster, **reboot
the writer** (`aws neptune reboot-db-instance`) for them to take effect.

The runner role also allows `neptune-db:ManageStatistics` so `--refresh-stats` works;
that is for the test stack only.

Delete it with the boto3-only teardown script (it
prints a plan and changes nothing until you add `--yes`):

```bash
python scripts/teardown.py <stack-name> --region us-east-1          # plan only
python scripts/teardown.py <stack-name> --region us-east-1 --yes    # delete everything
```

The buckets are versioned and the log bucket logs to itself, so the script turns
access logging off, empties every object version, deletes the stack, and retries if S3
delivers a late log object. Deleting the stack deletes the cluster and its data.

## Tests

```bash
pip install -r requirements-dev.txt
pytest
```

## References

- [Online index rebuild](https://docs.aws.amazon.com/neptune/latest/userguide/storage-index-rebuild.html)
- [GetStorageInfo](https://docs.aws.amazon.com/neptune/latest/userguide/storage-info.html)
- [Storage cleanup](https://docs.aws.amazon.com/neptune/latest/userguide/storage-cleanup.html)
- [Dictionary garbage collection](https://docs.aws.amazon.com/neptune/latest/userguide/storage-gc.html)
- [Engine 1.5.0.0](https://docs.aws.amazon.com/neptune/latest/userguide/engine-releases-1.5.0.0.html)

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the [LICENSE](LICENSE) file.
