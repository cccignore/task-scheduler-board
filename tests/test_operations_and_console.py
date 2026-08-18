"""Dashboard console features: operations ledger, managed workers, proofs, reset."""

import time

from tests.helpers import (
    assert_success,
    claim_task,
    complete_step,
    create_task,
    start_task,
)


def _log_events(client, **params):
    payload = assert_success(client.get("/api/logs", params=params))
    return payload["logs"]


def test_operation_ledger_records_the_full_lifecycle(client):
    task = create_task(
        client,
        "ledger-task",
        [{"name": "only", "overrides": {}}],
    )
    claim = claim_task(client, "ledger-worker")
    token = claim["claim_token"]
    start_task(client, task["id"], "ledger-worker", token)
    complete_step(client, task["id"], 1, "ledger-worker", token, True)
    # A same-value duplicate, then a contradictory one.
    complete_step(client, task["id"], 1, "ledger-worker", token, True)
    complete_step(client, task["id"], 1, "ledger-worker", token, False)

    logs = _log_events(client)
    events = [entry["event"] for entry in logs]
    for expected in (
        "task_created",
        "claim",
        "start",
        "step_report",
        "duplicate_report",
        "duplicate_conflict",
    ):
        assert expected in events, "missing {} in {}".format(expected, events)

    conflict = next(entry for entry in logs if entry["event"] == "duplicate_conflict")
    assert conflict["level"] == "warning"
    assert conflict["task_id"] == task["id"]
    assert conflict["step_sequence"] == 1
    assert conflict["worker_id"] == "ledger-worker"

    # Cursor mode returns only newer rows, newest first.
    newest_id = logs[0]["id"]
    assert _log_events(client, after_id=newest_id) == []
    older_cursor = logs[-1]["id"]
    incremental = _log_events(client, after_id=older_cursor)
    assert [entry["id"] for entry in incremental] == [
        entry["id"] for entry in logs if entry["id"] > older_cursor
    ]


def test_managed_workers_are_real_processes_and_execute_tasks(client):
    baseline = assert_success(client.get("/api/workers/managed"))
    assert baseline["workers"] == []
    assert baseline["max_workers"] == 10

    task = create_task(client, "managed-run", [{"name": "only", "overrides": {}}])
    spawned = assert_success(
        client.post(
            "/api/workers/managed",
            json={"count": 1, "step_seconds": 0.2},
        )
    )
    assert len(spawned["workers"]) == 1
    worker = spawned["workers"][0]
    assert worker["alive"] is True
    assert isinstance(worker["pid"], int)

    deadline = time.monotonic() + 30
    final_status = None
    while time.monotonic() < deadline:
        final_status = assert_success(client.get(f"/api/tasks/{task['id']}"))[
            "task"
        ]["status"]
        if final_status == "done":
            break
        time.sleep(0.3)
    assert final_status == "done"

    stopped = assert_success(client.post("/api/workers/managed/stop"))
    assert stopped["stopped"] == 1
    assert assert_success(client.get("/api/workers/managed"))["workers"] == []

    # The worker's actions were recorded in the shared cross-process ledger.
    events = [entry["event"] for entry in _log_events(client)]
    assert "claim" in events and "step_report" in events


def test_proof_endpoints_run_real_multiprocess_proofs(client):
    claim_proof = assert_success(
        client.post("/api/proofs/claim", json={"rounds": 2, "workers": 2})
    )
    assert claim_proof["kind"] == "claim"
    assert claim_proof["stats"]["passed"] is True
    assert claim_proof["stats"]["duplicate_claims"] == 0
    assert claim_proof["stats"]["start_method"] == "spawn"

    idempotency_proof = assert_success(client.post("/api/proofs/idempotency"))
    assert idempotency_proof["stats"]["passed"] is True
    assert idempotency_proof["stats"]["inserted_responses"] == 1
    assert idempotency_proof["stats"]["duplicate_responses"] == 4


def test_reset_clears_the_board_and_leaves_one_audit_row(client):
    assert_success(client.post("/api/demo"))
    assert assert_success(client.get("/api/tasks"))["tasks"]

    result = assert_success(client.post("/api/reset"))
    assert result["ok"] is True

    assert assert_success(client.get("/api/tasks"))["tasks"] == []
    assert assert_success(client.get("/api/groups"))["groups"] == []
    logs = _log_events(client)
    assert [entry["event"] for entry in logs] == ["reset"]
    assert logs[0]["level"] == "warning"
