"""Offline tests (no cluster needed). Run: pip install -r requirements-dev.txt && pytest"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import index_rebuild  # noqa: E402
from storage_info import components, quad_health  # noqa: E402
from verify import compare_storage  # noqa: E402

SAMPLE = {
    "totalStorageBytesUsed": 1000,
    "dictionaries": {"total": {"sizeInBytes": 300, "count": 10},
                     "blobs": {"sizeInBytes": 50, "count": 2}},
    "undoLogs": {"sizeInBytes": 100, "listSize": 1},
    "quads": {"PG": {"SPOG": {"sizeInBytes": 243_000_000, "count": 1_000_000},
                     "PZSOG": {"sizeInBytes": 1_000, "count": 10}}},
}


def test_components_do_not_double_count_blobs():
    c = components(SAMPLE)
    assert c["dictionaries"] == 300
    assert "blobs" not in str(c.keys())


def test_quad_health_bands():
    rows = {r["index"]: r for r in quad_health(SAMPLE)}
    assert rows["PG.SPOG"]["band"] == "heavily fragmented"
    assert rows["PG.SPOG"]["estReclaimableBytes"] == 243_000_000 - 52 * 1_000_000
    assert rows["PG.PZSOG"]["estReclaimableBytes"] is None


def test_compare_storage_lists_components():
    out = compare_storage(SAMPLE, {**SAMPLE, "totalStorageBytesUsed": 500})
    assert "undoLogs" in out and "quads.PG.SPOG" in out


def test_start_falls_back_to_active_rebuild_on_conflict(monkeypatch):
    from botocore.exceptions import ClientError

    class FakeClient:
        def start_index_rebuild(self, **kw):
            raise ClientError({"Error": {"Code": "ConflictException"}}, "StartIndexRebuild")

    monkeypatch.setattr(index_rebuild, "neptune_client", lambda *a, **k: FakeClient())
    monkeypatch.setattr(index_rebuild, "_token_param", lambda c: None)
    monkeypatch.setattr(index_rebuild, "_active_rebuild_id", lambda c, on_retry=None: "abc")
    assert index_rebuild.start_index_rebuild("x", "us-east-1", "QUADS") == "abc"


def test_with_backoff_does_not_retry_non_retriable():
    from botocore.exceptions import ClientError
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        raise ClientError({"Error": {"Code": "InternalFailureException"}}, "X")
    try:
        index_rebuild.with_backoff(fn, base_delay=0)
    except ClientError:
        pass
    assert calls["n"] == 1


# --- Real response shapes (ListIndexRebuilds returns ids only) ---------------------
class _FakeRebuildClient:
    def __init__(self, ids, statuses):
        self._ids, self._statuses = ids, statuses
        self.status_calls = []

    def list_index_rebuilds(self):
        return {"rebuildIds": self._ids}

    def get_index_rebuild_status(self, rebuildId):
        self.status_calls.append(rebuildId)
        return {"status": self._statuses[rebuildId], "scope": "QUADS", "progressPercentage": 40}


def test_active_rebuild_uses_rebuildIds_and_checks_each_status():
    c = _FakeRebuildClient(["old", "new"], {"old": "COMPLETED", "new": "REBUILDING"})
    rid, st = index_rebuild.active_rebuild(c)
    assert rid == "new" and st["status"] == "REBUILDING"
    assert c.status_calls == ["old", "new"]


def test_active_rebuild_none_when_all_terminal_or_empty():
    assert index_rebuild.active_rebuild(_FakeRebuildClient([], {})) is None
    done = _FakeRebuildClient(["a"], {"a": "FAILED"})
    assert index_rebuild.active_rebuild(done) is None


def test_start_falls_back_on_concurrent_modification(monkeypatch):
    from botocore.exceptions import ClientError

    class FakeClient(_FakeRebuildClient):
        def start_index_rebuild(self, **kw):
            raise ClientError({"Error": {"Code": "ConcurrentModificationException"}},
                              "StartIndexRebuild")

    fake = FakeClient(["r1"], {"r1": "PREPARING"})
    monkeypatch.setattr(index_rebuild, "neptune_client", lambda *a, **k: fake)
    monkeypatch.setattr(index_rebuild, "_token_param", lambda c: "clientToken")
    monkeypatch.setattr(index_rebuild.time, "sleep", lambda s: None)
    assert index_rebuild.start_index_rebuild("x", "us-east-1", "DICTIONARIES") == "r1"


def test_service_model_matches_code_assumptions():
    """Guards the field names the code relies on, against the installed botocore model."""
    import boto3
    m = boto3.client("neptunedata", region_name="us-east-1",
                     endpoint_url="https://localhost:8182").meta.service_model
    assert list(m.operation_model("ListIndexRebuilds").output_shape.members) == ["rebuildIds"]
    start_in = m.operation_model("StartIndexRebuild").input_shape.members
    assert "clientToken" in start_in and start_in["clientToken"].metadata.get("idempotencyToken")
    pg = m.operation_model("GetPropertygraphSummary").output_shape.members["payload"] \
        .members["graphSummary"].members
    assert {"numNodes", "numEdges"} <= set(pg)
    rdf = m.operation_model("GetRDFGraphSummary").output_shape.members["payload"] \
        .members["graphSummary"].members
    assert "numDistinctSubjects" in rdf
    assert "payload" in m.operation_model("ListLoaderJobs").output_shape.members


# --- Start call: attempted once on "already running", then ListIndexRebuilds fallback ---
import pytest  # noqa: E402


class _ConflictClient(_FakeRebuildClient):
    def __init__(self, code, ids, statuses):
        super().__init__(ids, statuses)
        self.code, self.start_calls = code, 0

    def start_index_rebuild(self, **kw):
        from botocore.exceptions import ClientError
        self.start_calls += 1
        raise ClientError({"Error": {"Code": self.code}}, "StartIndexRebuild")


@pytest.mark.parametrize("code", ["ConflictException", "ConcurrentModificationException"])
def test_start_is_attempted_once_then_falls_back(monkeypatch, code):
    fake = _ConflictClient(code, ["old", "live"], {"old": "COMPLETED", "live": "REBUILDING"})
    monkeypatch.setattr(index_rebuild, "neptune_client", lambda *a, **k: fake)
    monkeypatch.setattr(index_rebuild, "_token_param", lambda c: "clientToken")
    monkeypatch.setattr(index_rebuild.time, "sleep", lambda s: None)
    assert index_rebuild.start_index_rebuild("x", "us-east-1", "QUADS") == "live"
    assert fake.start_calls == 1                      # not retried with backoff
    assert fake.status_calls == ["old", "live"]       # ids, then status for each


@pytest.mark.parametrize("code", ["ConflictException", "ConcurrentModificationException"])
def test_start_raises_original_error_when_no_active_rebuild(monkeypatch, code):
    from botocore.exceptions import ClientError
    fake = _ConflictClient(code, ["old"], {"old": "COMPLETED"})
    monkeypatch.setattr(index_rebuild, "neptune_client", lambda *a, **k: fake)
    monkeypatch.setattr(index_rebuild, "_token_param", lambda c: "clientToken")
    monkeypatch.setattr(index_rebuild.time, "sleep", lambda s: None)
    with pytest.raises(ClientError) as ei:
        index_rebuild.start_index_rebuild("x", "us-east-1", "QUADS")
    assert ei.value.response["Error"]["Code"] == code
    assert fake.start_calls == 1


# --- with_backoff on_retry hook / non_retriable --------------------------------------
def _err(code):
    from botocore.exceptions import ClientError
    return ClientError({"Error": {"Code": code}}, "X")


def test_on_retry_sees_every_retry_including_timeouts(monkeypatch):
    from botocore.exceptions import ReadTimeoutError
    monkeypatch.setattr(index_rebuild.time, "sleep", lambda s: None)
    seq = [_err("CancelledByMaintenanceException"), ReadTimeoutError(endpoint_url="u"),
           _err("ConcurrentModificationException"), "ok"]
    seen = []

    def fn():
        x = seq.pop(0)
        if isinstance(x, Exception):
            raise x
        return x
    assert index_rebuild.with_backoff(fn, on_retry=lambda c, a, e: seen.append((c, a))) == "ok"
    assert seen == [("CancelledByMaintenanceException", 1), ("ReadTimeoutError", 2),
                    ("ConcurrentModificationException", 3)]


def test_on_retry_not_called_for_the_final_failed_attempt_or_non_retriable(monkeypatch):
    monkeypatch.setattr(index_rebuild.time, "sleep", lambda s: None)
    seen = []
    with pytest.raises(Exception):
        index_rebuild.with_backoff(lambda: (_ for _ in ()).throw(_err("CancelledByMaintenanceException")),
                                   max_attempts=3, on_retry=lambda c, a, e: seen.append(a))
    assert seen == [1, 2]            # 3 attempts -> 2 retries, no callback for the last failure
    seen.clear()
    with pytest.raises(Exception):
        index_rebuild.with_backoff(lambda: (_ for _ in ()).throw(_err("InternalFailureException")),
                                   on_retry=lambda c, a, e: seen.append(a))
    assert seen == []


def test_failing_on_retry_hook_does_not_change_behavior(monkeypatch):
    monkeypatch.setattr(index_rebuild.time, "sleep", lambda s: None)
    seq = [_err("CancelledByMaintenanceException"), "ok"]

    def fn():
        x = seq.pop(0)
        if isinstance(x, Exception):
            raise x
        return x

    def bad_hook(c, a, e):
        raise RuntimeError("boom")
    assert index_rebuild.with_backoff(fn, on_retry=bad_hook) == "ok"


def test_non_retriable_overrides_retriable_list():
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        raise _err("ConcurrentModificationException")
    with pytest.raises(Exception):
        index_rebuild.with_backoff(fn, non_retriable=("ConcurrentModificationException",))
    assert calls["n"] == 1


# --- storage_info: ratio (no percentage), statistics refresh, health JSON -----------
import datetime as _dt  # noqa: E402
import json  # noqa: E402
import storage_info  # noqa: E402


def test_dictionary_health_reports_a_ratio_and_no_percentage():
    info = {"dictionaries": {"total": {"sizeInBytes": 1, "count": 300}}}
    d = storage_info.dictionary_health(info, {"pg_nodes": 40, "pg_edges": 60})
    assert d == {"entries": 300, "live_elements": 100, "entries_per_live_element": 3.0}
    assert "dead_share_upper_bound" not in d
    empty = storage_info.dictionary_health({}, {})
    assert empty["entries_per_live_element"] is None


class _StatsClient:
    def __init__(self, polls, manage_id="s-new", manage_exc=None):
        self.polls, self.manage_id, self.manage_exc, self.manage_calls = list(polls), manage_id, manage_exc, []

    def manage_propertygraph_statistics(self, mode):
        self.manage_calls.append(mode)
        if self.manage_exc:
            raise self.manage_exc
        return {"payload": {"statisticsId": self.manage_id}}

    def get_propertygraph_statistics(self):
        return {"payload": self.polls.pop(0) if len(self.polls) > 1 else self.polls[0]}


def test_refresh_waits_until_the_new_statistics_id_is_visible():
    old = _dt.datetime(2020, 1, 1, tzinfo=_dt.timezone.utc)
    c = _StatsClient([{"statisticsId": "s-old", "date": old, "active": True},
                      {"statisticsId": "s-new", "date": _dt.datetime.now(_dt.timezone.utc),
                       "active": True}])
    out = storage_info.refresh_statistics("x", "us-east-1", client=c, sleep=lambda s: None)
    assert c.manage_calls == ["refresh"]
    assert out["completed"] is True and out["error"] is None
    assert [p["statisticsId"] for p in out["polls"]] == ["s-old", "s-new"]
    assert out["requestedAtUtc"].endswith("Z")


def test_refresh_times_out_and_records_it():
    old = _dt.datetime(2020, 1, 1, tzinfo=_dt.timezone.utc)
    c = _StatsClient([{"statisticsId": "s-old", "date": old, "active": True}])
    out = storage_info.refresh_statistics("x", "us-east-1", client=c, timeout_s=0,
                                          sleep=lambda s: None)
    assert out["completed"] is False and "timed out" in out["error"]


def test_refresh_records_access_denied_instead_of_raising():
    from botocore.exceptions import ClientError
    c = _StatsClient([{}], manage_exc=ClientError({"Error": {"Code": "AccessDeniedException"}},
                                                  "ManagePropertygraphStatistics"))
    out = storage_info.refresh_statistics("x", "us-east-1", client=c, sleep=lambda s: None)
    assert out["completed"] is False and "AccessDeniedException" in out["error"]


def test_graph_summary_elements_reads_counts_and_stats_time():
    when = _dt.datetime(2026, 11, 16, 1, 2, 3, tzinfo=_dt.timezone.utc)

    class C:
        def get_propertygraph_summary(self, mode):
            return {"payload": {"lastStatisticsComputationTime": when,
                                "graphSummary": {"numNodes": 5, "numEdges": 7}}}

        def get_rdf_graph_summary(self, mode):
            raise RuntimeError("no rdf data")
    s = storage_info.graph_summary_elements("x", "us-east-1", client=C())
    assert (s["pg_nodes"], s["pg_edges"], s["pg_stats_time"]) == (5, 7, "2026-11-16T01:02:03Z")
    assert "rdf_error" in s


def test_build_health_has_quad_and_dictionary_results_and_freshness():
    info = {**SAMPLE, "quads": {"PG": {"SPOG": {"sizeInBytes": 520, "count": 10}}}}
    summary = {"pg_nodes": 4, "pg_edges": 6, "pg_stats_time": "2026-11-16T10:00:00Z"}
    refresh = {"requestedAtUtc": "2026-11-16T09:59:00Z", "completed": True}
    h = storage_info.build_health(info, summary, refresh, captured_at="2026-11-16T10:01:00Z")
    assert h["capturedAtUtc"] == "2026-11-16T10:01:00Z"
    assert h["quadHealth"][0]["index"] == "PG.SPOG"
    assert h["dictionary"]["entries_per_live_element"] == 1.0
    assert h["summaryStatisticsAreFreshAfterRefresh"] is True
    assert "approximate" in h["caveat"]
    stale = storage_info.build_health(info, {**summary, "pg_stats_time": "2026-11-16T09:00:00Z"},
                                      refresh)
    assert stale["summaryStatisticsAreFreshAfterRefresh"] is False
    assert storage_info.build_health(info, summary)["summaryStatisticsAreFreshAfterRefresh"] is None
    import json
    json.dumps(h)  # must be JSON-serializable


# --- three reads, median, spread ----------------------------------------------------------
def _info(entries, total=1000):
    return {"totalStorageBytesUsed": total, "dictionaries": {"total": {"sizeInBytes": 1, "count": entries}}}


def test_read_storage_repeatedly_spaces_the_reads():
    sleeps, calls = [], iter([_info(10), _info(30), _info(20)])
    reads = storage_info.read_storage_repeatedly(lambda: next(calls), n=3, interval=60,
                                                 sleep=sleeps.append)
    assert sleeps == [60, 60]                      # n - 1 pauses, none before the first read
    assert [r["info"]["dictionaries"]["total"]["count"] for r in reads] == [10, 30, 20]
    assert all(r["capturedAtUtc"].endswith("Z") for r in reads)


def test_median_read_is_a_real_response_and_the_middle_count():
    reads = [{"capturedAtUtc": "t1", "info": _info(61_583_488)},
             {"capturedAtUtc": "t2", "info": _info(61_790_136)},
             {"capturedAtUtc": "t3", "info": _info(61_650_000)}]
    m = storage_info.median_read(reads)
    assert m["capturedAtUtc"] == "t3" and m is reads[2]            # 61,650,000 is the median
    s = storage_info.dictionary_read_stats(reads)
    assert s["median"] == 61_650_000 and s["min"] == 61_583_488 and s["max"] == 61_790_136
    assert s["spreadAbs"] == 206_648
    assert abs(s["spreadPctOfMedian"] - 206_648 / 61_650_000 * 100) < 1e-9
    assert [r["entries"] for r in s["reads"]] == [61_583_488, 61_790_136, 61_650_000]


def test_median_of_an_even_number_of_reads_takes_the_lower_one():
    reads = [{"capturedAtUtc": f"t{i}", "info": _info(c)} for i, c in enumerate([10, 40, 20, 30])]
    assert storage_info.median_read(reads)["info"]["dictionaries"]["total"]["count"] == 20
    assert storage_info.dictionary_read_stats(reads)["median"] == 20


def test_single_read_and_missing_counts_do_not_break_the_median():
    one = [{"capturedAtUtc": "t", "info": _info(5)}]
    assert storage_info.median_read(one) is one[0]
    s = storage_info.dictionary_read_stats(one)
    assert s["median"] == 5 and s["spreadAbs"] == 0
    none = [{"capturedAtUtc": "a", "info": {}}, {"capturedAtUtc": "b", "info": {}}]
    assert storage_info.median_read(none) is none[1]
    assert storage_info.dictionary_read_stats(none)["median"] is None


def test_health_records_every_read_and_the_median(tmp_path):
    reads = [{"capturedAtUtc": "2026-11-17T10:00:00Z", "info": _info(100, 5000)},
             {"capturedAtUtc": "2026-11-17T10:01:00Z", "info": _info(110, 5100)},
             {"capturedAtUtc": "2026-11-17T10:02:00Z", "info": _info(105, 5050)}]
    chosen = storage_info.median_read(reads)
    h = storage_info.build_health(chosen["info"], {"pg_nodes": 10, "pg_edges": 40},
                                  captured_at=reads[0]["capturedAtUtc"], reads=reads)
    assert h["dictionary"]["entries"] == 105 and h["totalStorageBytesUsed"] == 5050
    assert h["dictionaryReads"]["median"] == 105 and h["dictionaryReads"]["spreadAbs"] == 10
    assert [r["dictionaryEntries"] for r in h["storageReads"]] == [100, 110, 105]
    assert h["savedReadIsMedian"] is True and h["capturedAtUtc"] == "2026-11-17T10:00:00Z"
    json.dumps(h)
    assert "dictionaryReads" not in storage_info.build_health(_info(1), {"pg_nodes": 1})


def test_main_with_three_reads_saves_the_median_read(monkeypatch, tmp_path):
    counts = iter([61_000_000, 61_300_000, 61_150_000])
    monkeypatch.setattr(storage_info, "get_storage_info",
                        lambda *a, **k: _info(next(counts)))
    monkeypatch.setattr(storage_info, "graph_summary_elements",
                        lambda *a, **k: {"pg_nodes": 5, "pg_edges": 5, "pg_stats_time": "2026-11-17T09:59:00Z"})
    monkeypatch.setattr(storage_info.time, "sleep", lambda s: None)
    saved, health = tmp_path / "s.json", tmp_path / "h.json"
    monkeypatch.setattr(sys, "argv", ["storage_info.py", "--endpoint", "e", "--region", "r",
                                      "--reads", "3", "--read-interval", "60",
                                      "--save", str(saved), "--save-health", str(health)])
    storage_info.main()
    assert json.loads(saved.read_text())["dictionaries"]["total"]["count"] == 61_150_000
    h = json.loads(health.read_text())
    assert h["dictionaryReads"]["median"] == 61_150_000 and len(h["storageReads"]) == 3
