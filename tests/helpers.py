from typing import Any, Dict, Iterable, Optional


def assert_success(response, expected: Iterable[int] = (200, 201)) -> Any:
    assert response.status_code in expected, response.text
    if response.status_code == 204:
        return None
    return response.json()


def create_group(client, name: str, overrides: Optional[Dict[str, Any]] = None) -> dict:
    payload = assert_success(
        client.post(
            "/api/groups",
            json={"name": name, "overrides": overrides or {}},
        )
    )
    return payload["group"]


def update_group(client, group_id: int, **changes: Any) -> dict:
    payload = assert_success(client.patch(f"/api/groups/{group_id}", json=changes))
    return payload["group"]


def create_task(
    client,
    name: str,
    steps: list,
    *,
    base_parameters: Optional[Dict[str, Any]] = None,
    group_id: Optional[int] = None,
) -> dict:
    body: Dict[str, Any] = {
        "name": name,
        "base_parameters": base_parameters or {},
        "steps": steps,
    }
    if group_id is not None:
        body["group_id"] = group_id
    payload = assert_success(client.post("/api/tasks", json=body))
    return payload["task"]


def claim_task(client, worker_id: str) -> dict:
    payload = assert_success(
        client.post("/api/workers/claim-next", json={"worker_id": worker_id})
    )
    assert (payload["task"] is None) == (payload["claim_token"] is None)
    return payload


def start_task(client, task_id: int, worker_id: str, claim_token: str) -> dict:
    payload = assert_success(
        client.post(
            f"/api/tasks/{task_id}/start",
            json={"worker_id": worker_id, "claim_token": claim_token},
        )
    )
    return payload["task"]


def complete_step(
    client,
    task_id: int,
    sequence: int,
    worker_id: str,
    claim_token: str,
    success: bool,
) -> dict:
    payload = assert_success(
        client.post(
            f"/api/tasks/{task_id}/steps/{sequence}/complete",
            json={
                "worker_id": worker_id,
                "claim_token": claim_token,
                "success": success,
            },
        )
    )
    return payload["task"]


def get_task(client, task_id: int) -> dict:
    payload = assert_success(client.get(f"/api/tasks/{task_id}"))
    return payload["task"]
