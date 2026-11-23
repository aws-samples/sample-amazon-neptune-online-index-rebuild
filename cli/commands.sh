#!/usr/bin/env bash
#
# commands.sh - AWS CLI version of the workflow (neptunedata command family).
# Requires AWS CLI v2, engine 1.5.0.0+ (1.5.0.1 recommended), a provisioned writer,
# and network access to the WRITER endpoint from inside the VPC.
#
#   export NEPTUNE_ENDPOINT=<writer-or-cluster-endpoint>
#   export AWS_REGION=us-east-1
#   export NEPTUNE_CLUSTER_ID=my-neptune-cluster
#   export REBUILD_SCOPE=QUADS        # ALL | QUADS | DICTIONARIES (default ALL)
#   bash cli/commands.sh
set -euo pipefail

: "${NEPTUNE_ENDPOINT:?set NEPTUNE_ENDPOINT}"
: "${NEPTUNE_CLUSTER_ID:?set NEPTUNE_CLUSTER_ID}"
REGION="${AWS_REGION:-us-east-1}"
PORT="${NEPTUNE_PORT:-8182}"
SCOPE="${REBUILD_SCOPE:-ALL}"
URL="https://${NEPTUNE_ENDPOINT}:${PORT}"
NP=(--endpoint-url "${URL}" --region "${REGION}")

echo "=== Step 1: Diagnose - GetStorageInfo (saved to baseline.json) ==="
aws neptunedata get-storage-info "${NP[@]}" | tee baseline.json

echo "=== Step 2: Reclaim - start-index-rebuild (scope=${SCOPE}) ==="
REBUILD_ID="$(aws neptunedata start-index-rebuild "${NP[@]}" --scope "${SCOPE}" \
  --query rebuildId --output text)"
echo "rebuildId=${REBUILD_ID}"

echo "=== Step 3: Monitor ==="
# States: PREPARING -> REBUILDING -> COMPLETED | FAILED; CANCELLING -> CANCELLED.
while true; do
  if ! LINE="$(aws neptunedata get-index-rebuild-status "${NP[@]}" --rebuild-id "${REBUILD_ID}" \
      --query '[status, progressPercentage, estimatedEndTime, failureReason]' --output text)"; then
    echo "status poll failed (often a transient timeout during a rebuild); retrying"
    sleep 30; continue
  fi
  echo "status/progress/eta/failure: ${LINE}"
  case "${LINE%%[[:space:]]*}" in
    COMPLETED|FAILED|CANCELLED) break ;;
  esac
  sleep 30
done

echo "=== Step 4: Verify - GetStorageInfo again ==="
aws neptunedata get-storage-info "${NP[@]}" | tee after.json

echo "=== VolumeBytesUsed, last 72 h (hourly). Re-run over the next 1-3 days. ==="
aws cloudwatch get-metric-statistics --region "${REGION}" \
  --namespace AWS/Neptune --metric-name VolumeBytesUsed \
  --dimensions "Name=DBClusterIdentifier,Value=${NEPTUNE_CLUSTER_ID}" \
  --start-time "$(date -u -d '72 hours ago' +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v-72H +%Y-%m-%dT%H:%M:%SZ)" \
  --end-time "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --period 3600 --statistics Average

# Cancel (CANCELLING can take hours):
#   aws neptunedata cancel-index-rebuild "${NP[@]}" --rebuild-id "${REBUILD_ID}"
