"""
teardown.py - delete the blog test stack, including the versioned S3 buckets (boto3 only).

CloudFormation cannot delete a non-empty bucket, and the stack's buckets are versioned
(and the log bucket logs to itself), so this script:
  1. turns server access logging off on both buckets (so emptying them stops creating
     new log objects),
  2. deletes every object version and delete marker in both buckets,
  3. deletes the stack and waits for it,
  4. if the stack ends in DELETE_FAILED (S3 can deliver a late access-log object after the
     bucket was emptied), empties the buckets again and retries, up to --retries times.

It DELETES the cluster and its data and cannot be undone. Without --yes it only prints
what it would do.

  python scripts/teardown.py <stack-name> --region us-east-1            # plan only
  python scripts/teardown.py <stack-name> --region us-east-1 --yes      # do it
"""

import argparse
import sys
import time

import boto3
from botocore.exceptions import ClientError

BUCKET_LOGICAL_IDS = ("DataBucket", "AccessLogsBucket")


def log(msg):
    print(f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} {msg}", flush=True)


def _code(err):
    return err.response.get("Error", {}).get("Code", "")


def stack_status(cfn, stack):
    """Current stack status, or None when the stack no longer exists."""
    try:
        return cfn.describe_stacks(StackName=stack)["Stacks"][0]["StackStatus"]
    except ClientError as err:
        if "does not exist" in err.response.get("Error", {}).get("Message", ""):
            return None
        raise


def find_buckets(cfn, stack):
    """{logical id: bucket name} for the stack's buckets that exist."""
    found = {}
    for logical in BUCKET_LOGICAL_IDS:
        try:
            detail = cfn.describe_stack_resource(StackName=stack, LogicalResourceId=logical)
            found[logical] = detail["StackResourceDetail"]["PhysicalResourceId"]
        except ClientError as err:
            if _code(err) != "ValidationError":
                raise
            log(f"[skip] {logical} is not part of stack {stack}")
    return found


def disable_logging(s3, bucket):
    s3.put_bucket_logging(Bucket=bucket, BucketLoggingStatus={})
    log(f"[logging] server access logging disabled on {bucket}")


def empty_bucket(s3, bucket):
    """Delete every object version and delete marker. Returns the number deleted."""
    deleted = 0
    for page in s3.get_paginator("list_object_versions").paginate(Bucket=bucket):
        batch = [{"Key": o["Key"], "VersionId": o["VersionId"]}
                 for o in (page.get("Versions") or []) + (page.get("DeleteMarkers") or [])]
        for i in range(0, len(batch), 1000):
            resp = s3.delete_objects(Bucket=bucket, Delete={"Objects": batch[i:i + 1000],
                                                            "Quiet": True})
            if resp.get("Errors"):
                raise RuntimeError(f"S3 refused to remove objects in bucket {bucket}: {resp['Errors'][:3]}")
            deleted += len(batch[i:i + 1000])
    log(f"[empty] {bucket}: deleted {deleted} object versions/markers")
    return deleted


def failed_resources(cfn, stack):
    try:
        events = cfn.describe_stack_events(StackName=stack)["StackEvents"]
    except ClientError:
        return []
    return [f"{e['LogicalResourceId']}: {e.get('ResourceStatusReason', '')}"
            for e in events if e.get("ResourceStatus") == "DELETE_FAILED"][:5]


def wait_for_delete(cfn, stack, poll_s, timeout_s, sleep=time.sleep):
    """Poll until the stack is gone or DELETE_FAILED. Returns the final status (None = gone)."""
    waited = 0
    while True:
        status = stack_status(cfn, stack)
        if status is None or status == "DELETE_COMPLETE" or status == "DELETE_FAILED":
            return status
        if waited >= timeout_s:
            raise TimeoutError(f"stack {stack} still {status} after {timeout_s}s")
        sleep(poll_s)
        waited += poll_s


def teardown(stack, cfn, s3, retries=3, poll_s=30, timeout_s=3600, sleep=time.sleep):
    """Returns True when the stack is gone. Raises if it cannot be deleted."""
    status = stack_status(cfn, stack)
    if status is None:
        log(f"stack {stack} does not exist; nothing to do")
        return True
    log(f"stack {stack} is {status}")
    buckets = find_buckets(cfn, stack)
    for logical, bucket in buckets.items():
        disable_logging(s3, bucket)
    for attempt in range(1, retries + 2):
        for logical, bucket in buckets.items():
            empty_bucket(s3, bucket)
        log(f"[delete] delete_stack {stack} (attempt {attempt})")
        cfn.delete_stack(StackName=stack)
        final = wait_for_delete(cfn, stack, poll_s, timeout_s, sleep)
        if final is None or final == "DELETE_COMPLETE":
            log(f"stack {stack} deleted")
            return True
        log(f"[delete] DELETE_FAILED: {failed_resources(cfn, stack)}")
        if attempt > retries:
            break
        log("[delete] emptying the buckets again and retrying (late access-log objects?)")
        sleep(poll_s)
    raise RuntimeError(f"stack {stack} is DELETE_FAILED after {retries + 1} attempts; "
                       f"failed: {failed_resources(cfn, stack)}")


def main(argv=None, cfn=None, s3=None):
    ap = argparse.ArgumentParser(description="Delete the blog test stack and its buckets.")
    ap.add_argument("stack", help="CloudFormation stack name")
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--retries", type=int, default=3, help="retries after DELETE_FAILED")
    ap.add_argument("--poll-seconds", type=int, default=30)
    ap.add_argument("--yes", action="store_true", help="actually delete (otherwise plan only)")
    a = ap.parse_args(argv)
    cfn = cfn or boto3.client("cloudformation", region_name=a.region)
    s3 = s3 or boto3.client("s3", region_name=a.region)

    status = stack_status(cfn, a.stack)
    if status is None:
        log(f"stack {a.stack} does not exist in {a.region}; nothing to do")
        return 0
    buckets = find_buckets(cfn, a.stack)
    log(f"PLAN: stack {a.stack} ({status}) in {a.region}: disable logging on and empty "
        f"{list(buckets.values())}, then delete the stack (cluster, instances, VPC, roles).")
    if not a.yes:
        log("Nothing was changed. Re-run with --yes to delete.")
        return 2
    teardown(a.stack, cfn, s3, a.retries, a.poll_seconds)
    return 0


if __name__ == "__main__":
    sys.exit(main())
