"""Offline tests for the blog harness scripts (no cluster needed)."""
import csv
import os
import random
import sys

SCRIPTS = os.path.join(os.path.dirname(__file__), "..", "scripts")
sys.path.insert(0, SCRIPTS)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import benchmark_queries as bq  # noqa: E402
import stage_data_v2 as sd  # noqa: E402


def _read(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


# --- stage_data_v2: chunks never collide ------------------------------------------
def test_chunks_have_disjoint_ids_even_with_different_sizes(tmp_path):
    v0, e0 = sd.write_chunk(str(tmp_path), 0, 30, 20, seed=0)
    v1, e1 = sd.write_chunk(str(tmp_path), 1, 50, 20, seed=1)   # different size on purpose
    ids0, ids1 = {r["~id"] for r in _read(v0)}, {r["~id"] for r in _read(v1)}
    uid0, uid1 = {r["uid:String"] for r in _read(v0)}, {r["uid:String"] for r in _read(v1)}
    assert not ids0 & ids1 and not uid0 & uid1
    assert len(ids0) == 30 and len(ids1) == 50
    assert not {r["~id"] for r in _read(e0)} & {r["~id"] for r in _read(e1)}


def test_bucket_matches_vertex_number_mod_10_and_edges_stay_in_chunk(tmp_path):
    v, e = sd.write_chunk(str(tmp_path), 3, 40, 60, seed=3)
    rows = _read(v)
    for r in rows:
        n = int(r["uid:String"][2:])
        assert n // sd.CHUNK_ID_STRIDE == 3
        assert int(r["bucket:Int"]) == n % 10
    ids = {r["~id"] for r in rows}
    for r in _read(e):
        assert r["~from"] in ids and r["~to"] in ids


def test_write_chunk_rejects_bad_sizes(tmp_path):
    import pytest
    with pytest.raises(ValueError):
        sd.write_chunk(str(tmp_path), 0, 25, 5, seed=0)             # not a multiple of 10
    with pytest.raises(ValueError):
        sd.write_chunk(str(tmp_path), 0, sd.CHUNK_ID_STRIDE + 10, 5, seed=0)


# --- benchmark: uid pool uses surviving buckets only, same in every run -----------
def test_pool_covers_only_surviving_buckets_and_is_a_bijection():
    sizes = bq.parse_chunk_sizes(3, "40")
    assert sizes == [40, 40, 40]
    size = bq.uid_pool_size(sizes)
    assert size == 3 * 16
    nums = [bq.uid_number(k, sizes) for k in range(size)]
    assert len(set(nums)) == size
    assert all(n % 10 in (6, 7, 8, 9) for n in nums)
    assert all(0 <= n % sd.CHUNK_ID_STRIDE < 40 for n in nums)
    assert {n // sd.CHUNK_ID_STRIDE for n in nums} == {0, 1, 2}


def test_pool_handles_chunks_of_different_sizes():
    sizes = bq.parse_chunk_sizes(3, "40,40,20")          # like 2M, 2M, 1.5M at small scale
    assert bq.uid_pool_size(sizes) == 16 + 16 + 8
    nums = [bq.uid_number(k, sizes) for k in range(40)]
    assert len(set(nums)) == 40
    per_chunk = {c: sorted(n % sd.CHUNK_ID_STRIDE for n in nums if n // sd.CHUNK_ID_STRIDE == c)
                 for c in range(3)}
    assert [len(per_chunk[c]) for c in range(3)] == [16, 16, 8]
    assert max(per_chunk[2]) < 20 and max(per_chunk[0]) < 40
    import pytest
    with pytest.raises(IndexError):
        bq.uid_number(40, sizes)


def test_chunk_size_arguments_are_validated():
    import pytest
    assert bq.parse_chunk_sizes(2, "10,20") == [10, 20]
    for chunks, arg in ((2, "10,20,30"), (3, "10,20"), (1, "25"), (1, "0"),
                        (1, str(sd.CHUNK_ID_STRIDE + 10))):
        with pytest.raises(ValueError):
            bq.parse_chunk_sizes(chunks, arg)


def test_pool_uids_exist_in_generated_data_with_surviving_buckets(tmp_path):
    sizes = [40, 20]                                     # mixed chunk sizes
    existing = {}
    for c, vpc in enumerate(sizes):
        v, _ = sd.write_chunk(str(tmp_path), c, vpc, 5, seed=c)
        for r in _read(v):
            existing[r["uid:String"]] = int(r["bucket:Int"])
    for k in range(bq.uid_pool_size(sizes)):
        uid = bq.uid_string(bq.uid_number(k, sizes))
        assert uid in existing and existing[uid] in bq.SURVIVING_BUCKETS
    # and every surviving-bucket vertex that was generated is in the pool
    pool = {bq.uid_string(bq.uid_number(k, sizes)) for k in range(bq.uid_pool_size(sizes))}
    assert pool == {u for u, b in existing.items() if b in bq.SURVIVING_BUCKETS}


def test_same_seed_gives_the_same_query_sequence():
    def sequence(seed):
        rnd, out = random.Random(seed), []
        names = list(bq.QUERIES)
        for _ in range(200):
            name = rnd.choice(names)
            out.append((name, bq.make_params(rnd, bq.QUERIES[name][1], 1000, [2500])))
        return out
    assert sequence(7) == sequence(7)
    assert sequence(7) != sequence(8)


def test_each_query_gets_only_the_parameters_it_uses():
    rnd = random.Random(1)
    for name, (text, needed) in bq.QUERIES.items():
        params = bq.make_params(rnd, needed, 100, [1000])
        assert set(params) == set(needed)
        for p in params:
            assert f"${p}" in text


def test_geo_query_is_st_contains_against_a_fixed_polygon():
    text, needed = bq.QUERIES["geo_within"]
    assert "ST_Contains(" in text and "ST_Distance" not in text
    assert bq.GEO_POLYGON_WKT in text and needed == ()


# --- probe.py -----------------------------------------------------------------------
import io  # noqa: E402
import json  # noqa: E402
import threading  # noqa: E402

import probe  # noqa: E402


def _rec(sent, ms, err=None):
    return {"sent": sent, "latency_ms": ms, "error": err}


def test_nearest_rank_percentiles():
    xs = list(range(1, 101))
    assert probe.nearest_rank(xs, 50) == 50 and probe.nearest_rank(xs, 99) == 99
    assert probe.nearest_rank([7], 99) == 7 and probe.nearest_rank([], 50) is None


def test_summary_counts_errors_and_longest_gap_between_successes():
    t = 1_700_000_000.0
    recs = [_rec(t, 10), _rec(t + 5, 20), _rec(t + 10, 30000, "timeout"),
            _rec(t + 40, 30000, "timeout"), _rec(t + 75, 40), _rec(t + 80, 50)]
    s = probe.summarize(recs)
    assert (s["count"], s["ok"], s["errors_total"]) == (6, 4, 2)
    assert s["errors_by_code"] == {"timeout": 2}
    # successes complete at t+0.010, t+5.020, t+75.040, t+80.050 -> longest gap ~70.02 s
    assert s["longest_gap_s"] == 70.02
    assert s["p50_ms"] == 20 and s["p99_ms"] == 50
    assert s["longest_gap_from_utc"] < s["longest_gap_to_utc"]


def test_summary_with_no_or_one_success_has_no_gap():
    assert probe.summarize([])["count"] == 0
    s = probe.summarize([_rec(1.0, 5), _rec(6.0, 5, "InternalFailureException")])
    assert s["longest_gap_s"] is None and s["errors_by_code"] == {"InternalFailureException": 1}


def test_classify_timeouts_service_codes_and_other_errors():
    from botocore.exceptions import ClientError, ConnectTimeoutError, ReadTimeoutError
    assert probe.classify(ReadTimeoutError(endpoint_url="u")) == "timeout"
    assert probe.classify(ConnectTimeoutError(endpoint_url="u")) == "timeout"
    assert probe.classify(ClientError({"Error": {"Code": "CancelledByMaintenanceException"}},
                                      "X")) == "CancelledByMaintenanceException"
    assert probe.classify(RuntimeError("x")) == "RuntimeError"


def test_run_probe_logs_json_lines_and_stops_when_asked():
    from botocore.exceptions import ReadTimeoutError
    stop, calls = threading.Event(), {"n": 0}

    def send():
        calls["n"] += 1
        if calls["n"] == 2:
            raise ReadTimeoutError(endpoint_url="u")
        if calls["n"] == 4:
            stop.set()
    log = io.StringIO()
    records = probe.run_probe(send, log, interval=0.0, stop=stop)
    lines = [json.loads(x) for x in log.getvalue().splitlines()]
    assert len(lines) == len(records) == 4
    assert [x["error"] for x in lines] == [None, "timeout", None, None]
    assert set(lines[0]) == {"t", "latency_ms", "error"} and lines[0]["t"].endswith("Z")


def test_summary_can_be_rebuilt_from_a_log_file(tmp_path):
    p = tmp_path / "probe.log"
    p.write_text("\n".join(json.dumps(x) for x in [
        {"t": "2026-11-17T10:00:00.000000Z", "latency_ms": 12.0, "error": None},
        {"t": "2026-11-17T10:00:05.000000Z", "latency_ms": 9000.0, "error": "timeout"},
        {"t": "2026-11-17T10:00:10.000000Z", "latency_ms": 15.0, "error": None}]) + "\n")
    s = probe.summarize(probe.read_log(str(p)))
    assert s["count"] == 3 and s["errors_by_code"] == {"timeout": 1}
    assert s["longest_gap_s"] == 10.003


# --- run_reclaim.py: flags, retry counts by code (incl. timeouts), abort path --------
import time as _time  # noqa: E402
import run_reclaim as rr  # noqa: E402

_REAL_SLEEP = _time.sleep


def _setup_run(monkeypatch, tmp_path, status_fn, argv_extra=()):
    from botocore.exceptions import ClientError, ReadTimeoutError
    import index_rebuild
    for k in rr.RETRY_COUNTS:
        rr.RETRY_COUNTS[k].clear()
    n = {"calls": 0}
    started = {}

    class FakeRds:
        def describe_db_clusters(self, DBClusterIdentifier):
            return {"DBClusters": [{"EngineVersion": "1.5.0.1"}]}

    class FakeNeptune:
        def execute_open_cypher_query(self, **kw):
            n["calls"] += 1
            if n["calls"] == 1:
                raise ReadTimeoutError(endpoint_url="u")
            if n["calls"] == 2:
                raise ClientError({"Error": {"Code": "CancelledByMaintenanceException"}}, "Q")
            return {"results": [{"c": 1}]}

    monkeypatch.setattr(rr.boto3, "client", lambda service, **kw:
                        FakeRds() if service == "neptune" else FakeNeptune())
    monkeypatch.setattr(index_rebuild.time, "sleep", lambda s: _REAL_SLEEP(0.001))
    monkeypatch.setattr(rr.time, "sleep", lambda s: _REAL_SLEEP(0.001))
    monkeypatch.setattr(rr, "get_storage_info",
                        lambda *a, **k: {"totalStorageBytesUsed": 100,
                                         "dictionaries": {"total": {"sizeInBytes": 10, "count": 1}}})
    monkeypatch.setattr(rr, "cw_points", lambda *a, **k: [
        {"t": "2999-01-01T00:00:00Z", "gib": 1.0, "bytes": 1}, {"t": "2999-01-01T00:05:00Z",
                                                                  "gib": 1.0, "bytes": 1}])

    def fake_start(endpoint, region, scope="ALL", concurrency=None, write_buffer_size=None,
                   port=8182, client_token=None, on_retry=None):
        started.update(scope=scope, concurrency=concurrency, write_buffer_size=write_buffer_size)
        return "rid-1"
    monkeypatch.setattr(rr, "start_index_rebuild", fake_start)

    def fake_status(endpoint, region, rebuild_id, port=8182, on_retry=None):
        for _ in range(500):                      # let the workload make a few calls first
            if n["calls"] >= 4:
                break
            _REAL_SLEEP(0.01)
        return status_fn(on_retry)
    monkeypatch.setattr(rr, "get_index_rebuild_status", fake_status)
    out = tmp_path / "reclaim.json"
    monkeypatch.setattr(sys, "argv", ["run_reclaim.py", "--endpoint", "e", "--region", "r",
                                      "--cluster-id", "c", "--poll-seconds", "0",
                                      "--out", str(out), *argv_extra])
    return out, started


def test_run_reclaim_passes_flags_and_saves_retry_counts_by_code(monkeypatch, tmp_path):
    def status(on_retry):
        on_retry("ReadTimeoutError", 1, None)     # a retried status poll
        return {"status": "COMPLETED", "progressPercentage": 100}
    out, started = _setup_run(monkeypatch, tmp_path, status,
                              ["--scope", "DICTIONARIES", "--concurrency", "4",
                               "--write-buffer-size", "HIGH"])
    rr.main()
    assert started == {"scope": "DICTIONARIES", "concurrency": 4, "write_buffer_size": "HIGH"}
    R = json.loads(out.read_text())
    assert R["requested"] == {"scope": "DICTIONARIES", "concurrency": 4, "writeBufferSize": "HIGH"}
    rc = R["retry_counts"]
    assert rc["workload"] == {"ReadTimeoutError": 1, "CancelledByMaintenanceException": 1}
    assert rc["management"] == {"ReadTimeoutError": 1}
    assert R["workload_stats"]["queries"] >= 2 and "workload_stats_so_far" not in R
    assert R["final_status"]["status"] == "COMPLETED"
    assert R["raw_error_counts"] is not None


def test_run_reclaim_keeps_stats_and_retry_counts_when_monitoring_fails(monkeypatch, tmp_path):
    import pytest

    def status(on_retry):
        raise RuntimeError("status call blew up")
    out, _ = _setup_run(monkeypatch, tmp_path, status)
    with pytest.raises(RuntimeError):
        rr.main()
    R = json.loads(out.read_text())
    assert any("aborted: RuntimeError" in e for e in R["errors"])
    assert R["retry_counts"]["workload"] == {"ReadTimeoutError": 1,
                                             "CancelledByMaintenanceException": 1}
    assert R["workload_stats"]["queries"] >= 2           # stats saved, not lost
    assert R["rebuild_id"] == "rid-1"


# --- teardown.py ---------------------------------------------------------------------
import teardown as td  # noqa: E402


class _FakeCfn:
    def __init__(self, statuses, buckets=None):
        self.statuses, self.deletes = list(statuses), 0
        self.buckets = buckets if buckets is not None else {"DataBucket": "data-b",
                                                            "AccessLogsBucket": "logs-b"}
        self.events = []

    def describe_stacks(self, StackName):
        from botocore.exceptions import ClientError
        s = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
        if s is None:
            raise ClientError({"Error": {"Code": "ValidationError",
                                          "Message": f"Stack with id {StackName} does not exist"}},
                              "DescribeStacks")
        return {"Stacks": [{"StackStatus": s}]}

    def describe_stack_resource(self, StackName, LogicalResourceId):
        from botocore.exceptions import ClientError
        if LogicalResourceId not in self.buckets:
            raise ClientError({"Error": {"Code": "ValidationError", "Message": "no such resource"}},
                              "DescribeStackResource")
        return {"StackResourceDetail": {"PhysicalResourceId": self.buckets[LogicalResourceId]}}

    def delete_stack(self, StackName):
        self.deletes += 1

    def describe_stack_events(self, StackName):
        return {"StackEvents": [{"LogicalResourceId": "AccessLogsBucket",
                                 "ResourceStatus": "DELETE_FAILED",
                                 "ResourceStatusReason": "bucket not empty"}]}


class _FakeS3:
    def __init__(self, versions_per_bucket=2500):
        self.calls, self.n = [], versions_per_bucket
        self.remaining = {}

    def put_bucket_logging(self, Bucket, BucketLoggingStatus):
        assert BucketLoggingStatus == {}
        self.calls.append(("logging_off", Bucket))

    def get_paginator(self, name):
        s3 = self

        class P:
            def paginate(self, Bucket):
                n = s3.remaining.setdefault(Bucket, s3.n)
                half = n // 2
                yield {"Versions": [{"Key": f"k{i}", "VersionId": "v"} for i in range(half)],
                       "DeleteMarkers": [{"Key": f"d{i}", "VersionId": "m"} for i in range(n - half)]}
        return P()

    def delete_objects(self, Bucket, Delete):
        self.calls.append(("delete", Bucket, len(Delete["Objects"])))
        self.remaining[Bucket] = 0
        return {}


def test_teardown_refuses_without_yes_and_changes_nothing():
    cfn, s3 = _FakeCfn(["CREATE_COMPLETE"]), _FakeS3()
    assert td.main(["my-stack"], cfn=cfn, s3=s3) == 2
    assert cfn.deletes == 0 and s3.calls == []


def test_teardown_turns_logging_off_then_empties_in_batches_then_deletes():
    cfn = _FakeCfn(["CREATE_COMPLETE", "DELETE_IN_PROGRESS", None])
    s3 = _FakeS3(2500)
    assert td.teardown("my-stack", cfn, s3, poll_s=0, sleep=lambda s: None) is True
    kinds = [c[0] for c in s3.calls]
    assert kinds[:2] == ["logging_off", "logging_off"]            # before any delete
    deletes = [c for c in s3.calls if c[0] == "delete"]
    # 2 buckets x 2500 versions, at most 1000 per delete_objects call
    assert sorted(c[2] for c in deletes) == [500, 500, 1000, 1000, 1000, 1000]
    assert cfn.deletes == 1


def test_teardown_retries_after_delete_failed_and_removes_a_late_log_object():
    class LateCfn(_FakeCfn):
        def delete_stack(self, StackName):
            super().delete_stack(StackName)
            if self.deletes == 1:                 # S3 delivers a late access-log object
                s3.remaining["logs-b"] = 3
    cfn = LateCfn(["CREATE_COMPLETE", "DELETE_FAILED", "DELETE_IN_PROGRESS", None])
    s3 = _FakeS3(10)
    assert td.teardown("s", cfn, s3, retries=2, poll_s=0, sleep=lambda s: None) is True
    assert cfn.deletes == 2
    deletes = [c for c in s3.calls if c[0] == "delete"]
    assert [(c[1], c[2]) for c in deletes] == [("data-b", 10), ("logs-b", 10), ("logs-b", 3)]
    assert sum(1 for c in s3.calls if c[0] == "logging_off") == 2      # logging off only once


def test_teardown_gives_up_after_retries():
    import pytest
    cfn, s3 = _FakeCfn(["CREATE_COMPLETE", "DELETE_FAILED"]), _FakeS3(4)
    with pytest.raises(RuntimeError, match="DELETE_FAILED after 2 attempts"):
        td.teardown("s", cfn, s3, retries=1, poll_s=0, sleep=lambda s: None)
    assert cfn.deletes == 2


def test_teardown_handles_missing_stack_and_missing_log_bucket():
    assert td.teardown("gone", _FakeCfn([None]), _FakeS3(), sleep=lambda s: None) is True
    cfn = _FakeCfn(["CREATE_COMPLETE", None], buckets={"DataBucket": "only-b"})   # old template
    s3 = _FakeS3(3)
    assert td.teardown("old", cfn, s3, poll_s=0, sleep=lambda s: None) is True
    assert {c[1] for c in s3.calls} == {"only-b"}


# --- benchmark: instance class, pre-window, deferred metrics --------------------------------
import argparse as _argparse  # noqa: E402
import datetime as _dt2  # noqa: E402
import itertools as _it  # noqa: E402


class _FakeCw:
    def __init__(self):
        self.calls = []

    def get_metric_statistics(self, **kw):
        self.calls.append(kw)
        pts = [{"Timestamp": kw["StartTime"] + _dt2.timedelta(minutes=i),
                **{s: 10.0 * (i + 1) for s in kw["Statistics"]}} for i in range(2)]
        return {"Datapoints": list(reversed(pts))}          # out of order on purpose


def test_collect_metrics_takes_the_five_minutes_before_the_run():
    cw = _FakeCw()
    start = _dt2.datetime(2026, 11, 16, 23, 30, 0, tzinfo=_dt2.timezone.utc)
    end = start + _dt2.timedelta(seconds=600)
    m = bq.collect_metrics(cw, "writer-1", "cluster-1", start, end)
    pre, during = m["preWindow"], m["duringRun"]
    assert (pre["startUtc"], pre["endUtc"]) == ("2026-11-16T23:25:00Z", "2026-11-16T23:30:00Z")
    assert (during["startUtc"], during["endUtc"]) == ("2026-11-16T23:30:00Z", "2026-11-16T23:40:00Z")
    assert set(pre) == {"startUtc", "endUtc", "FreeableMemory", "BufferCacheHitRatio"}
    assert [p["t"] for p in pre["FreeableMemory"]] == ["2026-11-16T23:25:00Z", "2026-11-16T23:26:00Z"]
    assert set(pre["FreeableMemory"][0]) == {"t", "average", "minimum"}
    assert set(during["VolumeReadIOPs"][0]) == {"t", "average", "sum"}
    dims = {(c["MetricName"], c["Dimensions"][0]["Name"], c["Dimensions"][0]["Value"]) for c in cw.calls}
    assert ("FreeableMemory", "DBInstanceIdentifier", "writer-1") in dims
    assert ("BufferCacheHitRatio", "DBInstanceIdentifier", "writer-1") in dims
    assert ("VolumeReadIOPs", "DBClusterIdentifier", "cluster-1") in dims
    assert all(c["Period"] == 60 for c in cw.calls)
    assert m["BufferCacheHitRatio"] == 15.0 and m["VolumeReadIOPs"] == 15.0


class _FakeNeptuneData:
    def __init__(self):
        self.seen = []

    def execute_open_cypher_query(self, **kw):
        self.seen.append((kw["openCypherQuery"], kw.get("parameters")))
        return {"results": [{"c": 1}]}


def _pass(seed, **kw):
    ticks = _it.count(0, 1)                    # fake clock: +1 s per reading
    client = _FakeNeptuneData()
    lat, empty, errors, start, end = bq.execute_pass(client, [40, 40], seed, 30,
                                                      clock=lambda: next(ticks),
                                                      perf=lambda: next(ticks) * 0.001, **kw)
    return client, lat, empty, errors


def test_pass_sequence_is_identical_for_the_same_seed_and_geo_can_be_skipped():
    a, b, c = _pass(7)[0], _pass(7)[0], _pass(8)[0]
    assert a.seen == b.seen and a.seen != c.seen and len(a.seen) > 5
    assert any("ST_Contains" in q for q, _ in a.seen)
    no_geo = _pass(7, skip_geo=True)[0]
    assert not any("ST_Contains" in q for q, _ in no_geo.seen)


def _args():
    return _argparse.Namespace(label="before-cold", region="us-east-1", cluster_id="c1",
                               instance_id="w1", duration=600, seed=7, skip_geo=False)


def test_result_records_the_instance_class_and_defers_metrics(tmp_path):
    client, lat, empty, errors = _pass(7)
    start = _dt2.datetime(2026, 11, 16, 23, 30, tzinfo=_dt2.timezone.utc)
    end = start + _dt2.timedelta(seconds=600)
    inst = {"DBInstanceClass": "db.r6g.large", "DBInstanceStatus": "available"}
    out = bq.build_result(_args(), [40, 40], inst, lat, empty, errors, start, end)
    assert out["instanceClass"] == "db.r6g.large" and out["metricsCollected"] is False
    assert out["seed"] == 7 and out["uidPool"]["size"] == 32 and out["verticesPerChunk"] == [40, 40]
    assert out["startUtc"] == "2026-11-16T23:30:00Z" and out["endUtc"] == "2026-11-16T23:40:00Z"
    path = tmp_path / "bench.json"
    path.write_text(json.dumps(out))
    done = bq.collect_into(str(path), "us-east-1", cw=_FakeCw())
    saved = json.loads(path.read_text())
    assert saved["metricsCollected"] is True and saved["metricsCollectedAtUtc"].endswith("Z")
    assert saved["preWindow"]["startUtc"] == "2026-11-16T23:25:00Z"
    assert saved["instanceClass"] == "db.r6g.large" and done["BufferCacheHitRatio"] == 15.0


def test_wide_scan_is_opt_in_and_leaves_the_other_four_queries_unchanged():
    plain, lat0, _, _ = _pass(7)
    assert "wide_scan" not in lat0 and not any("LIMIT 50000" in q for q, _ in plain.seen)
    wide, lat1, empty1, _ = _pass(7, wide_scan=True, wide_every=5)
    wide_calls = [(q, p) for q, p in wide.seen if "LIMIT 50000" in q]
    regular = [(q, p) for q, p in wide.seen if "LIMIT 50000" not in q]
    assert wide_calls and len(lat1["wide_scan"]) == len(wide_calls)
    assert regular == plain.seen[:len(regular)]          # same sequence, same parameters
    for _, p in wide_calls:
        prm = json.loads(p)
        assert prm["b"] in (6, 7, 8, 9) and 0 <= prm["lo"] < (1 << 38)
    assert set(lat0) < set(lat1)
