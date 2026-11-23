"""
index_rebuild.py - Start, monitor, and cancel an Amazon Neptune online index rebuild.

Requires Neptune engine release 1.5.0.0 or later (1.5.0.1 recommended). Send every
request to the WRITER endpoint; the writer must be a provisioned instance (online
index rebuild is not supported on a Neptune Serverless writer).

Retry note: this module retries the rebuild *management* calls. During a rebuild,
your APPLICATION queries can also fail with CancelledByMaintenanceException; see
src/query_retry.py for a pattern to copy into your Gremlin/openCypher/SPARQL code.

Docs: https://docs.aws.amazon.com/neptune/latest/userguide/storage-index-rebuild.html
"""

import argparse
import random
import time
import uuid

import boto3
from botocore.exceptions import ClientError, ConnectTimeoutError, ReadTimeoutError

VALID_SCOPES = ("ALL", "QUADS", "DICTIONARIES")
VALID_WRITE_BUFFER_SIZES = ("LOW", "MEDIUM", "HIGH")
TERMINAL_STATES = {"COMPLETED", "FAILED", "CANCELLED"}
ALL_STATES = ("PREPARING", "REBUILDING", "CANCELLING", "COMPLETED", "FAILED", "CANCELLED")

RETRIABLE_ERRORS = (
    "CancelledByMaintenanceException",
    "ConcurrentModificationException",
    "ThrottlingException",       # name used in the rebuild API docs
    "TooManyRequestsException",  # name in the botocore neptunedata service model
)

# StartIndexRebuild errors that can mean "a rebuild is already running". The docs
# say 409 ConflictException; the botocore model lists ConcurrentModificationException
# for this operation instead, so both are treated as a hint to look for an active rebuild.
ALREADY_RUNNING_CODES = ("ConflictException", "ConcurrentModificationException")


def neptune_client(endpoint, region, port=8182):
    return boto3.client("neptunedata", endpoint_url=f"https://{endpoint}:{port}",
                        region_name=region)


def _sleep(attempt, base_delay, max_delay):
    delay = random.uniform(0, min(max_delay, base_delay * (2 ** (attempt - 1))))
    time.sleep(delay)
    return delay


def _notify(on_retry, code, attempt, err):
    """Call the optional on_retry hook; a failing hook never changes retry behavior."""
    if on_retry is None:
        return
    try:
        on_retry(code, attempt, err)
    except Exception:  # noqa: BLE001
        pass


def with_backoff(fn, *, max_attempts=6, base_delay=1.0, max_delay=30.0,
                 retry_timeouts=True, non_retriable=(), on_retry=None):
    """Call fn() with exponential backoff and full jitter on retriable errors.

    Only use retry_timeouts=True for idempotent calls (status, list, or a start
    call that carries a client token). Non-retriable errors propagate at once.

    non_retriable: error codes to raise immediately even if they are normally retried.
    on_retry: optional callable(error_code, attempt, exception), called once per
        retry just before the backoff sleep. error_code is the service error code, or
        the exception class name for client timeouts (ReadTimeoutError,
        ConnectTimeoutError). Default None: behavior is unchanged.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            return fn()
        except (ReadTimeoutError, ConnectTimeoutError) as err:
            if not retry_timeouts or attempt >= max_attempts:
                raise
            _notify(on_retry, type(err).__name__, attempt, err)
            d = _sleep(attempt, base_delay, max_delay)
            print(f"[retry] {type(err).__name__} {attempt}/{max_attempts}; {d:.1f}s")
        except ClientError as err:
            code = err.response.get("Error", {}).get("Code", "")
            if code in non_retriable or code not in RETRIABLE_ERRORS or attempt >= max_attempts:
                raise
            _notify(on_retry, code, attempt, err)
            d = _sleep(attempt, base_delay, max_delay)
            print(f"[retry] {code} {attempt}/{max_attempts}; {d:.1f}s")


def _token_param(client):
    """Return the StartIndexRebuild client-token parameter name, if the model has one."""
    members = client.meta.service_model.operation_model("StartIndexRebuild").input_shape.members
    for name in members:
        if name.lower() == "clienttoken":
            return name
    return None


def list_index_rebuilds(client, on_retry=None):
    """Return the rebuild ids known to the writer (only the most recent is retained).

    ListIndexRebuilds returns just {"rebuildIds": [...]}: ids only, no status.
    """
    resp = with_backoff(lambda: client.list_index_rebuilds(), on_retry=on_retry)
    return list(resp.get("rebuildIds") or [])


def active_rebuild(client, on_retry=None):
    """Return (rebuildId, status_response) for a rebuild that is not in a terminal
    state, or None. Needs one GetIndexRebuildStatus call per listed id."""
    for rid in list_index_rebuilds(client, on_retry):
        st = with_backoff(lambda rid=rid: client.get_index_rebuild_status(rebuildId=rid),
                          on_retry=on_retry)
        if (st.get("status") or "").upper() not in TERMINAL_STATES:
            return rid, st
    return None


def _active_rebuild_id(client, on_retry=None):
    found = active_rebuild(client, on_retry)
    return found[0] if found else None


def start_index_rebuild(endpoint, region, scope="ALL", concurrency=None,
                        write_buffer_size=None, port=8182, client_token=None,
                        on_retry=None):
    """Start an online index rebuild and return its rebuildId.

    A client token makes retries of the start call safe. If the service reports
    that a rebuild is already running (ConflictException, or
    ConcurrentModificationException), the start call is NOT retried: the active
    rebuild's id is looked up with ListIndexRebuilds (then GetIndexRebuildStatus
    for each id) instead of being lost. If no active rebuild is found, the original
    error is raised. Note the returned rebuild may have a different scope than the
    one requested; check its status if that matters.

    write_buffer_size tracks concurrent QUERY writes during the rebuild. It has no
    effect on bulk loads: bulk load and fast reset requests are rejected while a
    rebuild runs.
    """
    scope = scope.upper()
    if scope not in VALID_SCOPES:
        raise ValueError(f"scope must be one of {VALID_SCOPES}, got {scope!r}")
    client = neptune_client(endpoint, region, port)
    params = {"scope": scope}
    if concurrency is not None:
        params["concurrency"] = int(concurrency)
    if write_buffer_size is not None:
        wbs = write_buffer_size.upper()
        if wbs not in VALID_WRITE_BUFFER_SIZES:
            raise ValueError(f"write_buffer_size must be one of {VALID_WRITE_BUFFER_SIZES}")
        params["writeBufferSize"] = wbs
    token_name = _token_param(client)
    if token_name:
        params[token_name] = client_token or str(uuid.uuid4())
    try:
        resp = with_backoff(lambda: client.start_index_rebuild(**params),
                            retry_timeouts=bool(token_name),
                            non_retriable=ALREADY_RUNNING_CODES, on_retry=on_retry)
        return resp.get("rebuildId")
    except ClientError as err:
        if err.response.get("Error", {}).get("Code") in ALREADY_RUNNING_CODES:
            active = _active_rebuild_id(client, on_retry)
            if active:
                print(f"[start] a rebuild is already running; using rebuildId={active}")
                return active
        raise


def get_index_rebuild_status(endpoint, region, rebuild_id, port=8182, on_retry=None):
    client = neptune_client(endpoint, region, port)
    return with_backoff(lambda: client.get_index_rebuild_status(rebuildId=rebuild_id),
                        on_retry=on_retry)


def cancel_index_rebuild(endpoint, region, rebuild_id, port=8182, on_retry=None):
    """Request cancellation. CANCELLING can take a long time (up to hours)."""
    client = neptune_client(endpoint, region, port)
    return with_backoff(lambda: client.cancel_index_rebuild(rebuildId=rebuild_id),
                        on_retry=on_retry)


def wait_for_completion(endpoint, region, rebuild_id, poll_seconds=30, port=8182,
                        on_retry=None):
    """Poll until the rebuild reaches COMPLETED, FAILED, or CANCELLED.

    Prints status, progressPercentage, and estimatedEndTime (approximate) on each
    poll. Only the most recent rebuild is retained and its record is lost on
    restart or failover; a ResourceNotFoundException is reported as LOST.
    """
    while True:
        try:
            st = get_index_rebuild_status(endpoint, region, rebuild_id, port, on_retry)
        except ClientError as err:
            if err.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
                print(f"[status] {rebuild_id}: record not found (restart or failover?)")
                return {"rebuildId": rebuild_id, "status": "LOST"}
            raise
        state = (st.get("status") or "").upper()
        print(f"[status] {rebuild_id}: {state} progress={st.get('progressPercentage')}% "
              f"eta={st.get('estimatedEndTime')}")
        if state in TERMINAL_STATES:
            if state == "FAILED":
                print(f"[status] failureReason={st.get('failureReason')}")
            return st
        time.sleep(poll_seconds)


def main():
    ap = argparse.ArgumentParser(description="Run a Neptune online index rebuild.")
    ap.add_argument("--endpoint", required=True, help="Neptune WRITER endpoint hostname")
    ap.add_argument("--region", required=True)
    ap.add_argument("--port", type=int, default=8182)
    ap.add_argument("--scope", default="ALL", choices=VALID_SCOPES)
    ap.add_argument("--concurrency", type=int)
    ap.add_argument("--write-buffer-size", choices=VALID_WRITE_BUFFER_SIZES)
    ap.add_argument("--wait", action="store_true")
    ap.add_argument("--poll-seconds", type=int, default=30)
    a = ap.parse_args()
    rid = start_index_rebuild(a.endpoint, a.region, a.scope, a.concurrency,
                              a.write_buffer_size, a.port)
    print(f"Started index rebuild (scope={a.scope}); rebuildId={rid}")
    if a.wait:
        final = wait_for_completion(a.endpoint, a.region, rid, a.poll_seconds, a.port)
        print(f"Final status: {final.get('status')}")


if __name__ == "__main__":
    main()
