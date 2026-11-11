"""
index_rebuild.py — Run an Amazon Neptune online index rebuild with safe retry handling.

Online index rebuild (Neptune engine release 1.5.0.0+) defragments quad and/or
dictionary indexes and reclaims unused storage space *while the database stays
fully operational* — concurrent reads and writes continue across Gremlin,
openCypher, and SPARQL.

Key operational detail: requests can be cancelled when the storage layer briefly
enters maintenance mode during a rebuild. The service raises
CancelledByMaintenanceException, which should be treated like
ConcurrentModificationException — i.e. it is retriable. This module wraps calls
in exponential backoff so your application stays resilient during a rebuild.

Docs:
  https://docs.aws.amazon.com/neptune/latest/userguide/storage-index-rebuild.html
  https://docs.aws.amazon.com/neptune/latest/userguide/storage-cleanup.html

Requires: Neptune engine release 1.5.0.0 or later.
"""

import argparse
import random
import time

import boto3
from botocore.exceptions import ClientError

# Target scopes accepted by the rebuild API.
VALID_SCOPES = ("ALL", "QUADS", "DICTIONARIES")
VALID_WRITE_BUFFER_SIZES = ("LOW", "MEDIUM", "HIGH")

# Exceptions that are safe to retry — the storage layer was briefly in
# maintenance mode. Treat CancelledByMaintenanceException like
# ConcurrentModificationException.
RETRIABLE_ERRORS = (
    "CancelledByMaintenanceException",
    "ConcurrentModificationException",
)


def _neptune_client(endpoint, region, port=8182):
    return boto3.client(
        "neptunedata",
        endpoint_url=f"https://{endpoint}:{port}",
        region_name=region,
    )


def with_backoff(fn, *, max_attempts=6, base_delay=1.0, max_delay=30.0):
    """Call fn() with exponential backoff + jitter on retriable Neptune errors.

    Retries only on CancelledByMaintenanceException / ConcurrentModificationException.
    All other errors propagate immediately.

    Args:
        fn: Zero-argument callable that performs the Neptune request.
        max_attempts: Maximum number of attempts before giving up.
        base_delay: Initial backoff delay in seconds.
        max_delay: Cap on any single backoff delay in seconds.

    Returns:
        Whatever fn() returns on success.

    Raises:
        ClientError: Re-raised if a non-retriable error occurs or attempts are
                     exhausted.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            return fn()
        except ClientError as err:
            code = err.response.get("Error", {}).get("Code", "")
            if code not in RETRIABLE_ERRORS or attempt >= max_attempts:
                raise
            # Exponential backoff with full jitter.
            delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
            delay = random.uniform(0, delay)
            print(
                f"[retry] {code} on attempt {attempt}/{max_attempts}; "
                f"backing off {delay:.1f}s"
            )
            time.sleep(delay)


def start_index_rebuild(
    endpoint,
    region,
    scope="ALL",
    concurrency=None,
    write_buffer_size=None,
    port=8182,
):
    """Start an online index rebuild and return the rebuild id.

    Args:
        endpoint: Neptune endpoint hostname.
        region: AWS region.
        scope: Scope of the rebuild — one of ALL, QUADS, DICTIONARIES.
        concurrency: Optional number of indexes to rebuild in parallel
            (integer, 1..number of vCPUs on the writer; defaults to 1).
            Higher values can shorten the rebuild but increase CPU and I/O load.
        write_buffer_size: Optional write-buffer size — one of LOW, MEDIUM, HIGH
            (defaults to LOW). Use a larger value under heavy write traffic or
            bulk loads.
        port: Neptune port.

    Returns:
        str: The index-rebuild id (rebuildId), used to poll status or cancel.
    """
    scope = scope.upper()
    if scope not in VALID_SCOPES:
        raise ValueError(f"scope must be one of {VALID_SCOPES}, got {scope!r}")

    client = _neptune_client(endpoint, region, port)

    params = {"scope": scope}
    if concurrency is not None:
        params["concurrency"] = concurrency
    if write_buffer_size is not None:
        wbs = write_buffer_size.upper()
        if wbs not in VALID_WRITE_BUFFER_SIZES:
            raise ValueError(
                f"write_buffer_size must be one of {VALID_WRITE_BUFFER_SIZES}, "
                f"got {write_buffer_size!r}")
        params["writeBufferSize"] = wbs

    resp = with_backoff(lambda: client.start_index_rebuild(**params))
    return resp.get("rebuildId")


def get_index_rebuild_status(endpoint, region, operation_id, port=8182):
    """Return the current status of an index rebuild operation."""
    client = _neptune_client(endpoint, region, port)
    return with_backoff(
        lambda: client.get_index_rebuild_status(rebuildId=operation_id)
    )


def cancel_index_rebuild(endpoint, region, operation_id, port=8182):
    """Cancel an in-progress index rebuild operation."""
    client = _neptune_client(endpoint, region, port)
    return with_backoff(
        lambda: client.cancel_index_rebuild(rebuildId=operation_id)
    )


def wait_for_completion(endpoint, region, operation_id, poll_seconds=30, port=8182):
    """Poll an index rebuild until it reaches a terminal state.

    Args:
        poll_seconds: Seconds to wait between status polls.

    Returns:
        dict: The final status response.
    """
    terminal = {"COMPLETED", "FAILED", "CANCELLED"}
    while True:
        status = get_index_rebuild_status(endpoint, region, operation_id, port)
        state = (status.get("status") or status.get("state") or "").upper()
        print(f"[status] operation {operation_id}: {state or 'UNKNOWN'}")
        if state in terminal:
            return status
        time.sleep(poll_seconds)


def main():
    parser = argparse.ArgumentParser(description="Run a Neptune online index rebuild.")
    parser.add_argument("--endpoint", required=True, help="Neptune endpoint hostname")
    parser.add_argument("--region", required=True, help="AWS region, e.g. us-east-1")
    parser.add_argument("--port", type=int, default=8182, help="Neptune port")
    parser.add_argument(
        "--scope",
        default="ALL",
        choices=VALID_SCOPES,
        help="Scope of the rebuild (default ALL)",
    )
    parser.add_argument(
        "--concurrency", type=int, default=None,
        help="Number of indexes to rebuild in parallel (1..vCPUs)",
    )
    parser.add_argument(
        "--write-buffer-size", default=None, choices=VALID_WRITE_BUFFER_SIZES,
        help="Write buffer size (LOW, MEDIUM, HIGH; default LOW)",
    )
    parser.add_argument(
        "--wait", action="store_true", help="Poll until the rebuild completes"
    )
    parser.add_argument("--poll-seconds", type=int, default=30, help="Status poll interval")
    args = parser.parse_args()

    op_id = start_index_rebuild(
        args.endpoint,
        args.region,
        scope=args.scope,
        concurrency=args.concurrency,
        write_buffer_size=args.write_buffer_size,
        port=args.port,
    )
    print(f"Started index rebuild (scope={args.scope}); rebuildId={op_id}")

    if args.wait:
        final = wait_for_completion(
            args.endpoint, args.region, op_id, args.poll_seconds, args.port
        )
        print(f"Final state: {final.get('status') or final.get('state')}")


if __name__ == "__main__":
    main()
