from tests.helpers import (
    assert_success,
    claim_task,
    complete_step,
    create_group,
    create_task,
    get_task,
    start_task,
)


def _logs(task: dict) -> list:
    return task["execution_logs"]


def test_success_state_machine_order_and_idempotent_retry(client):
    task = create_task(
        client,
        "state-machine-success",
        [
            {"name": "one", "overrides": {}},
            {"name": "two", "overrides": {}},
        ],
    )
    assert task["status"] == "pending"

    response = client.post(
        f"/api/tasks/{task['id']}/start", json={"worker_id": "worker-a"}
    )
    assert response.status_code == 409

    claimed = claim_task(client, "worker-a")
    assert claimed["id"] == task["id"]
    assert claimed["status"] == "claimed"
    assert claimed["claimed_by"] == "worker-a"

    response = client.post(
        f"/api/tasks/{task['id']}/start", json={"worker_id": "worker-b"}
    )
    assert response.status_code == 409

    start_task(client, task["id"], "worker-a")
    running = get_task(client, task["id"])
    assert running["status"] == "running"
    assert [step["status"] for step in running["steps"]] == ["running", "pending"]

    out_of_order = client.post(
        f"/api/tasks/{task['id']}/steps/2/complete",
        json={"worker_id": "worker-a", "success": True},
    )
    assert out_of_order.status_code == 409

    wrong_owner = client.post(
        f"/api/tasks/{task['id']}/steps/1/complete",
        json={"worker_id": "worker-b", "success": True},
    )
    assert wrong_owner.status_code == 409

    complete_step(client, task["id"], 1, "worker-a", True)
    after_first = get_task(client, task["id"])
    assert after_first["status"] == "running"
    assert [step["status"] for step in after_first["steps"]] == ["done", "running"]
    assert len(_logs(after_first)) == 1
    assert _logs(after_first)[0]["success"] is True

    # A late contradictory retry is a successful no-op, not a destructive update.
    complete_step(client, task["id"], 1, "worker-a", False)
    after_retry = get_task(client, task["id"])
    assert [step["status"] for step in after_retry["steps"]] == ["done", "running"]
    assert len(_logs(after_retry)) == 1
    assert _logs(after_retry)[0]["success"] is True

    complete_step(client, task["id"], 2, "worker-a", True)
    done = get_task(client, task["id"])
    assert done["status"] == "done"
    assert [step["status"] for step in done["steps"]] == ["done", "done"]
    assert len(_logs(done)) == 2


def test_failed_step_terminates_task(client):
    task = create_task(
        client,
        "state-machine-failure",
        [
            {"name": "will-fail", "overrides": {}},
            {"name": "must-not-start", "overrides": {}},
        ],
    )
    claim_task(client, "failure-worker")
    start_task(client, task["id"], "failure-worker")
    complete_step(client, task["id"], 1, "failure-worker", False)

    failed = get_task(client, task["id"])
    assert failed["status"] == "failed"
    assert [step["status"] for step in failed["steps"]] == ["failed", "pending"]
    assert len(_logs(failed)) == 1
    assert _logs(failed)[0]["success"] is False


def test_api_and_dashboard_smoke(client):
    assert assert_success(client.get("/api/health")) == {"status": "ok"}
    initial = assert_success(client.get("/api/tasks"))
    assert initial["tasks"] == []
    assert claim_task(client, "idle-worker") is None

    group = create_group(client, "smoke-group", {"channel": "email"})
    task = create_task(
        client,
        "smoke-task",
        [{"name": "send", "overrides": {}}],
        base_parameters={"message": "hello"},
        group_id=group["id"],
    )
    listed = assert_success(client.get("/api/tasks"))["tasks"]
    assert [item["id"] for item in listed] == [task["id"]]
    assert get_task(client, task["id"])["name"] == "smoke-task"

    demo = assert_success(client.post("/api/demo"))
    assert demo["running_task"]["status"] == "running"
    assert demo["pending_task"]["status"] == "pending"

    page = client.get("/")
    assert page.status_code == 200
    assert "text/html" in page.headers["content-type"]
    assert "任务" in page.text
