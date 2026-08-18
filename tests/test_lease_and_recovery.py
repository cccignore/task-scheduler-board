"""Lease-based recovery: claim deadlines, token rotation, and manual requeue.

The lease is a *start* deadline: a claimed-but-never-started task holds no
external side effects, so reclaiming it automatically is safe.  A running
task is never auto-reclaimed (a rotated token fences database writes but not
an already-sent message); stuck running tasks need an explicit operator
requeue, which is also covered here.
"""

import sqlite3

from app import services

from tests.helpers import (
    assert_success,
    create_group,
    create_task,
    get_task,
    update_group,
)


def test_lease_covers_claim_only_and_is_cleared_at_start(client, database_path):
    task = create_task(
        client,
        "lease-window",
        [
            {"name": "one", "overrides": {}},
            {"name": "two", "overrides": {}},
        ],
    )
    claim = services.claim_next_task(database_path, "worker-a", lease_seconds=900)
    assert claim["task"]["id"] == task["id"]
    assert claim["task"]["lease_expires_at"] is not None
    assert "claim_token" not in claim["task"]

    started = services.start_task(
        database_path, task["id"], "worker-a", claim["claim_token"]
    )
    # Once running, the task may have external side effects: no deadline, no
    # automatic requeue.
    assert started["lease_expires_at"] is None

    services.complete_step(
        database_path, task["id"], 1, "worker-a", claim["claim_token"], True
    )
    services.complete_step(
        database_path, task["id"], 2, "worker-a", claim["claim_token"], True
    )
    finished = get_task(client, task["id"])
    assert finished["status"] == "done"
    assert finished["lease_expires_at"] is None


def test_expired_claim_lease_is_reclaimed_and_stale_credentials_stop_working(
    client, database_path, monkeypatch
):
    # Zero-length leases make the claim instantly reclaimable, so the test
    # exercises the crash-before-start path without waiting for wall-clock time.
    monkeypatch.setenv("TASKBOARD_LEASE_SECONDS", "0")
    task = create_task(client, "lease-expiry", [{"name": "only", "overrides": {}}])

    stale = services.claim_next_task(database_path, "crashed-worker")
    assert stale["task"]["id"] == task["id"]

    fresh = services.claim_next_task(database_path, "recovery-worker", lease_seconds=900)
    assert fresh is not None
    assert fresh["task"]["id"] == task["id"]
    assert fresh["task"]["claimed_by"] == "recovery-worker"
    assert fresh["claim_token"] != stale["claim_token"]

    # The rotated-out token cannot start, and cannot report either.
    response = client.post(
        f"/api/tasks/{task['id']}/start",
        json={"worker_id": "crashed-worker", "claim_token": stale["claim_token"]},
    )
    assert response.status_code == 409

    services.start_task(
        database_path, task["id"], "recovery-worker", fresh["claim_token"]
    )
    late_report = client.post(
        f"/api/tasks/{task['id']}/steps/1/complete",
        json={
            "worker_id": "crashed-worker",
            "claim_token": stale["claim_token"],
            "success": False,
        },
    )
    assert late_report.status_code == 409

    services.complete_step(
        database_path, task["id"], 1, "recovery-worker", fresh["claim_token"], True
    )
    assert get_task(client, task["id"])["status"] == "done"


def test_running_task_is_never_auto_reclaimed(client, database_path, monkeypatch):
    monkeypatch.setenv("TASKBOARD_LEASE_SECONDS", "0")
    task = create_task(client, "no-overlap", [{"name": "only", "overrides": {}}])
    claim = services.claim_next_task(database_path, "worker-a")
    services.start_task(database_path, task["id"], "worker-a", claim["claim_token"])

    # Even with an artificially expired lease planted on the running row,
    # claim-next must not steal it: the old worker may still be mid-side-effect.
    connection = sqlite3.connect(database_path)
    try:
        connection.execute(
            "UPDATE tasks SET lease_expires_at = '2000-01-01T00:00:00.000Z' WHERE id = ?",
            (task["id"],),
        )
        connection.commit()
    finally:
        connection.close()

    assert services.claim_next_task(database_path, "worker-b") is None
    unchanged = get_task(client, task["id"])
    assert unchanged["status"] == "running"
    assert unchanged["claimed_by"] == "worker-a"

    # The original owner finishes normally: no overlap, no lost work.
    services.complete_step(
        database_path, task["id"], 1, "worker-a", claim["claim_token"], True
    )
    assert get_task(client, task["id"])["status"] == "done"


def test_manual_requeue_rotates_credentials_and_resumes_from_pending_step(
    client, database_path
):
    group = create_group(client, "requeue-group", {"channel": "email"})
    task = create_task(
        client,
        "manual-requeue",
        [
            {"name": "one", "overrides": {"stage": "one"}},
            {"name": "two", "overrides": {"stage": ""}},
            {"name": "three", "overrides": {"stage": "three"}},
        ],
        base_parameters={"stage": "base", "channel": "sms"},
        group_id=group["id"],
    )

    first = services.claim_next_task(database_path, "worker-a", lease_seconds=900)
    services.start_task(database_path, task["id"], "worker-a", first["claim_token"])
    services.complete_step(
        database_path, task["id"], 1, "worker-a", first["claim_token"], True
    )
    resolved_before = [
        step["resolved_parameters"] for step in get_task(client, task["id"])["steps"]
    ]

    # The worker vanished mid-run; an operator decides the side effects are
    # safe to resume and requeues explicitly.
    requeued = assert_success(client.post(f"/api/tasks/{task['id']}/requeue"))["task"]
    assert requeued["status"] == "pending"
    assert requeued["claimed_by"] is None
    assert [step["status"] for step in requeued["steps"]] == [
        "done",
        "pending",
        "pending",
    ]
    assert requeued["execution_logs"][0]["step_sequence"] == 1

    # A terminal task cannot be requeued.
    conflict = client.post("/api/tasks/999999/requeue")
    assert conflict.status_code == 404

    second = services.claim_next_task(database_path, "worker-b", lease_seconds=900)
    assert second["task"]["id"] == task["id"]
    assert second["claim_token"] != first["claim_token"]

    # The old worker's credentials are dead after the requeue-claim cycle.
    stale_report = client.post(
        f"/api/tasks/{task['id']}/steps/2/complete",
        json={
            "worker_id": "worker-a",
            "claim_token": first["claim_token"],
            "success": True,
        },
    )
    assert stale_report.status_code == 409

    # The L2 snapshot froze at the first start; later group edits stay invisible.
    update_group(client, group["id"], overrides={"channel": "push"})
    resumed = services.start_task(
        database_path, task["id"], "worker-b", second["claim_token"]
    )
    assert [step["status"] for step in resumed["steps"]] == [
        "done",
        "running",
        "pending",
    ]
    assert resumed["group_parameters_snapshot"] == {"channel": "email"}
    assert [
        step["resolved_parameters"] for step in resumed["steps"]
    ] == resolved_before

    services.complete_step(
        database_path, task["id"], 2, "worker-b", second["claim_token"], True
    )
    services.complete_step(
        database_path, task["id"], 3, "worker-b", second["claim_token"], True
    )
    finished = get_task(client, task["id"])
    assert finished["status"] == "done"
    assert [log["step_sequence"] for log in finished["execution_logs"]] == [1, 2, 3]

    done_conflict = client.post(f"/api/tasks/{task['id']}/requeue")
    assert done_conflict.status_code == 409


def test_live_lease_is_not_reclaimed(client, database_path):
    create_task(client, "healthy-lease", [{"name": "only", "overrides": {}}])
    healthy = services.claim_next_task(database_path, "worker-a", lease_seconds=900)
    assert healthy is not None

    # A second worker must not steal a task whose lease is still valid.
    assert services.claim_next_task(database_path, "worker-b") is None
    unchanged = get_task(client, healthy["task"]["id"])
    assert unchanged["claimed_by"] == "worker-a"
    assert unchanged["status"] == "claimed"
