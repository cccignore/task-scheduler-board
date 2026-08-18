"""Lease-based crash recovery: reclaim, token rotation, and resume."""

from app import services

from tests.helpers import create_group, create_task, get_task, update_group


def test_claim_grants_lease_and_successful_reports_renew_it(client, database_path):
    task = create_task(
        client,
        "lease-renewal",
        [
            {"name": "one", "overrides": {}},
            {"name": "two", "overrides": {}},
        ],
    )
    claim = services.claim_next_task(database_path, "worker-a", lease_seconds=900)
    assert claim["task"]["id"] == task["id"]
    lease_after_claim = claim["task"]["lease_expires_at"]
    assert lease_after_claim is not None
    assert "claim_token" not in claim["task"]

    started = services.start_task(
        database_path, task["id"], "worker-a", claim["claim_token"]
    )
    lease_after_start = started["lease_expires_at"]
    assert lease_after_start >= lease_after_claim

    report = services.complete_step(
        database_path, task["id"], 1, "worker-a", claim["claim_token"], True
    )
    assert report["task"]["lease_expires_at"] >= lease_after_start

    services.complete_step(
        database_path, task["id"], 2, "worker-a", claim["claim_token"], True
    )
    finished = get_task(client, task["id"])
    assert finished["status"] == "done"
    assert finished["lease_expires_at"] is None


def test_expired_lease_is_reclaimed_and_stale_credentials_stop_working(
    client, database_path, monkeypatch
):
    # Zero-length leases make every renewal instantly reclaimable, so the test
    # exercises the crash path without waiting for wall-clock time.
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


def test_reclaimed_running_task_resumes_from_first_pending_step(
    client, database_path, monkeypatch
):
    monkeypatch.setenv("TASKBOARD_LEASE_SECONDS", "0")
    group = create_group(client, "lease-group", {"channel": "email"})
    task = create_task(
        client,
        "lease-resume",
        [
            {"name": "one", "overrides": {"stage": "one"}},
            {"name": "two", "overrides": {"stage": ""}},
            {"name": "three", "overrides": {"stage": "three"}},
        ],
        base_parameters={"stage": "base", "channel": "sms"},
        group_id=group["id"],
    )

    first = services.claim_next_task(database_path, "worker-a")
    services.start_task(database_path, task["id"], "worker-a", first["claim_token"])
    services.complete_step(
        database_path, task["id"], 1, "worker-a", first["claim_token"], True
    )
    resolved_before = [
        step["resolved_parameters"] for step in get_task(client, task["id"])["steps"]
    ]

    # The lease has already expired, so the next claim reclaims the task even
    # though step 2 was mid-flight; the finished step 1 must survive.
    second = services.claim_next_task(database_path, "worker-b", lease_seconds=900)
    assert second["task"]["id"] == task["id"]
    reclaimed = second["task"]
    assert reclaimed["status"] == "claimed"
    assert [step["status"] for step in reclaimed["steps"]] == [
        "done",
        "pending",
        "pending",
    ]
    assert reclaimed["execution_logs"][0]["step_sequence"] == 1

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
    assert finished["execution_logs"][0] == reclaimed["execution_logs"][0]


def test_live_lease_is_not_reclaimed(client, database_path):
    create_task(client, "healthy-lease", [{"name": "only", "overrides": {}}])
    healthy = services.claim_next_task(database_path, "worker-a", lease_seconds=900)
    assert healthy is not None

    # A second worker must not steal a task whose lease is still valid.
    assert services.claim_next_task(database_path, "worker-b") is None
    unchanged = get_task(client, healthy["task"]["id"])
    assert unchanged["claimed_by"] == "worker-a"
    assert unchanged["status"] == "claimed"
