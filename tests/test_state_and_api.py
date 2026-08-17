import threading
from datetime import datetime

from app import services

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
    forged_token = "forged-claim-token"

    response = client.post(
        f"/api/tasks/{task['id']}/start",
        json={"worker_id": "worker-a", "claim_token": forged_token},
    )
    assert response.status_code == 409

    claim = claim_task(client, "worker-a")
    claimed = claim["task"]
    claim_token = claim["claim_token"]
    assert claimed["id"] == task["id"]
    assert claimed["status"] == "claimed"
    assert claimed["claimed_by"] == "worker-a"
    assert "claim_token" not in claimed

    response = client.post(
        f"/api/tasks/{task['id']}/start",
        json={"worker_id": "worker-b", "claim_token": claim_token},
    )
    assert response.status_code == 409

    # Reusing the public worker_id without the server-issued token cannot take over.
    response = client.post(
        f"/api/tasks/{task['id']}/start",
        json={"worker_id": "worker-a", "claim_token": forged_token},
    )
    assert response.status_code == 409

    start_task(client, task["id"], "worker-a", claim_token)
    running = get_task(client, task["id"])
    assert running["status"] == "running"
    assert [step["status"] for step in running["steps"]] == ["running", "pending"]

    out_of_order = client.post(
        f"/api/tasks/{task['id']}/steps/2/complete",
        json={
            "worker_id": "worker-a",
            "claim_token": claim_token,
            "success": True,
        },
    )
    assert out_of_order.status_code == 409

    wrong_owner = client.post(
        f"/api/tasks/{task['id']}/steps/1/complete",
        json={
            "worker_id": "worker-b",
            "claim_token": claim_token,
            "success": True,
        },
    )
    assert wrong_owner.status_code == 409

    forged_owner = client.post(
        f"/api/tasks/{task['id']}/steps/1/complete",
        json={
            "worker_id": "worker-a",
            "claim_token": forged_token,
            "success": True,
        },
    )
    assert forged_owner.status_code == 409

    complete_step(client, task["id"], 1, "worker-a", claim_token, True)
    after_first = get_task(client, task["id"])
    assert after_first["status"] == "running"
    assert [step["status"] for step in after_first["steps"]] == ["done", "running"]
    assert len(_logs(after_first)) == 1
    canonical_log = _logs(after_first)[0]
    assert canonical_log["task_id"] == task["id"]
    assert canonical_log["step_sequence"] == 1
    assert canonical_log["success"] is True
    completed_at = datetime.fromisoformat(
        canonical_log["completed_at"].replace("Z", "+00:00")
    )
    assert completed_at.utcoffset() is not None

    # A late contradictory retry is a successful no-op, not a destructive update.
    complete_step(client, task["id"], 1, "worker-a", claim_token, False)
    after_retry = get_task(client, task["id"])
    assert [step["status"] for step in after_retry["steps"]] == ["done", "running"]
    assert len(_logs(after_retry)) == 1
    assert _logs(after_retry)[0] == canonical_log

    complete_step(client, task["id"], 2, "worker-a", claim_token, True)
    done = get_task(client, task["id"])
    assert done["status"] == "done"
    assert [step["status"] for step in done["steps"]] == ["done", "done"]
    assert len(_logs(done)) == 2


def test_failed_step_terminates_task(client):
    task = create_task(
        client,
        "state-machine-failure",
        [
            {"name": "will-fail", "overrides": {"stage": "failed-step"}},
            {"name": "must-not-start", "overrides": {"stage": "future-step"}},
        ],
        base_parameters={"stage": "base"},
    )
    claim = claim_task(client, "failure-worker")
    claim_token = claim["claim_token"]
    start_task(client, task["id"], "failure-worker", claim_token)
    complete_step(client, task["id"], 1, "failure-worker", claim_token, False)

    # First-write-wins also applies when failure is the canonical first report.
    complete_step(client, task["id"], 1, "failure-worker", claim_token, True)

    failed = get_task(client, task["id"])
    assert failed["status"] == "failed"
    assert [step["status"] for step in failed["steps"]] == ["failed", "pending"]
    assert len(_logs(failed)) == 1
    assert _logs(failed)[0]["success"] is False
    assert failed["resolved_parameters"] == {"stage": "failed-step"}


def test_task_detail_is_one_consistent_read_snapshot(
    client, database_path, monkeypatch
):
    task = create_task(
        client,
        "read-snapshot",
        [{"name": "only-step", "overrides": {}}],
    )
    claim = claim_task(client, "snapshot-reader-worker")
    claim_token = claim["claim_token"]
    start_task(
        client,
        task["id"],
        "snapshot-reader-worker",
        claim_token,
    )

    task_row_read = threading.Event()
    continue_reader = threading.Event()
    original_connect = services.connect

    class PausingConnection:
        def __init__(self, connection):
            self._connection = connection

        @property
        def in_transaction(self):
            return self._connection.in_transaction

        def execute(self, sql, parameters=()):
            cursor = self._connection.execute(sql, parameters)
            if "SELECT t.*" in sql:
                task_row_read.set()
                assert continue_reader.wait(timeout=10)
            return cursor

        def commit(self):
            return self._connection.commit()

        def rollback(self):
            return self._connection.rollback()

        def close(self):
            return self._connection.close()

    def controlled_connect(path):
        connection = original_connect(path)
        if threading.current_thread().name == "snapshot-reader":
            return PausingConnection(connection)
        return connection

    monkeypatch.setattr(services, "connect", controlled_connect)
    observed = {}

    def read_during_completion():
        try:
            observed["task"] = services.get_task(database_path, task["id"])
        except BaseException as exc:
            observed["error"] = exc

    reader = threading.Thread(target=read_during_completion, name="snapshot-reader")
    reader.start()
    assert task_row_read.wait(timeout=10)

    # WAL allows this writer to commit while the reader retains its old snapshot.
    services.complete_step(
        database_path,
        task["id"],
        1,
        "snapshot-reader-worker",
        claim_token,
        True,
    )
    continue_reader.set()
    reader.join(timeout=10)
    assert not reader.is_alive()
    assert "error" not in observed

    snapshot = observed["task"]
    assert snapshot["status"] == "running"
    assert snapshot["steps"][0]["status"] == "running"
    assert snapshot["execution_logs"] == []
    assert services.get_task(database_path, task["id"])["status"] == "done"


def test_api_and_dashboard_smoke(client):
    assert assert_success(client.get("/api/health")) == {"status": "ok"}
    initial = assert_success(client.get("/api/tasks"))
    assert initial["tasks"] == []
    idle_claim = claim_task(client, "idle-worker")
    assert idle_claim == {"task": None, "claim_token": None}

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
    assert demo["claim_token"]
    assert "claim_token" not in demo["task"]

    page = client.get("/")
    assert page.status_code == 200
    assert "text/html" in page.headers["content-type"]
    assert "任务" in page.text
