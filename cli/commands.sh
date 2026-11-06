#!/usr/bin/env bash
#
# commands.sh — Equivalent AWS CLI invocations for the Neptune storage-reclaim
# workflow. These mirror the boto3 code in ../src and use the `neptunedata`
# command family (Neptune engine release 1.5.0.0 or later).
#
# Prereqs:
#   - AWS CLI v2 configured with credentials that can reach the cluster.
#   - Network access to the Neptune endpoint (run from within the VPC, e.g. an
#     EC2 bastion or a VPC-attached environment).
#   - IAM auth (if enabled) with neptune-db:* permissions for the actions below.
#
# Replace the placeholders before running.
set -euo pipefail

ENDPOINT="${NEPTUNE_ENDPOINT:-my-cluster.cluster-abc123.us-east-1.neptune.amazonaws.com}"
REGION="${AWS_REGION:-us-east-1}"
PORT="${NEPTUNE_PORT:-8182}"
CLUSTER_ID="${NEPTUNE_CLUSTER_ID:-my-neptune-cluster}"
TARGET="${REBUILD_TARGET:-ALL}"   # ALL | QUADS | DICTIONARIES

NEPTUNE_URL="https://${ENDPOINT}:${PORT}"

echo "=== Step 1: Diagnose (baseline) — GetStorageInfo ==="
aws neptunedata get-storage-info \
  --endpoint-url "${NEPTUNE_URL}" \
  --region "${REGION}"

echo "=== Step 2: Reclaim — start online index rebuild (target=${TARGET}) ==="
OP_JSON="$(aws neptunedata start-index-rebuild \
  --endpoint-url "${NEPTUNE_URL}" \
  --region "${REGION}" \
  --target "${TARGET}")"
echo "${OP_JSON}"
OP_ID="$(echo "${OP_JSON}" | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("operationId") or d.get("id",""))')"
echo "operationId=${OP_ID}"

echo "=== Step 3: Monitor — poll index rebuild status ==="
# Poll until the status is a terminal state (COMPLETED/FAILED/CANCELLED).
while true; do
  STATUS="$(aws neptunedata get-index-rebuild-status \
    --endpoint-url "${NEPTUNE_URL}" \
    --region "${REGION}" \
    --operation-id "${OP_ID}")"
  STATE="$(echo "${STATUS}" | python3 -c 'import sys,json;d=json.load(sys.stdin);print((d.get("status") or d.get("state") or "").upper())')"
  echo "status: ${STATE}"
  case "${STATE}" in
    COMPLETED|FAILED|CANCELLED) break ;;
  esac
  sleep 30
done

echo "=== Step 4: Verify — GetStorageInfo again + CloudWatch VolumeBytesUsed ==="
aws neptunedata get-storage-info \
  --endpoint-url "${NEPTUNE_URL}" \
  --region "${REGION}"

aws cloudwatch get-metric-statistics \
  --namespace AWS/Neptune \
  --metric-name VolumeBytesUsed \
  --dimensions "Name=DBClusterIdentifier,Value=${CLUSTER_ID}" \
  --start-time "$(date -u -d '6 hours ago' +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v-6H +%Y-%m-%dT%H:%M:%SZ)" \
  --end-time "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --period 300 \
  --statistics Average \
  --region "${REGION}"

# To cancel an in-progress rebuild:
#   aws neptunedata cancel-index-rebuild \
#     --endpoint-url "${NEPTUNE_URL}" --region "${REGION}" --operation-id "${OP_ID}"
