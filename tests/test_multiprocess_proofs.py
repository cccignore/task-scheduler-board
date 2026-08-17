from scripts.run_concurrency_proof import run_claim_proof
from scripts.run_idempotency_proof import run_idempotency_proof


def test_spawn_processes_claim_each_task_exactly_once(database_path):
    stats = run_claim_proof(
        database_path,
        rounds=12,
        workers=6,
        quiet=True,
    )

    assert stats["start_method"] == "spawn"
    assert stats["claim_attempts"] == 72
    assert stats["unique_winners"] == 12
    assert stats["duplicate_claims"] == 0
    assert stats["anomalous_rounds"] == 0
    assert stats["errors"] == []
    assert stats["pending_tasks"] == 0


def test_five_spawn_processes_write_one_immutable_success_log(database_path):
    stats = run_idempotency_proof(database_path, processes=5, quiet=True)

    assert stats["start_method"] == "spawn"
    assert stats["processes"] == 5
    assert stats["inserted_responses"] == 1
    assert stats["duplicate_responses"] == 4
    assert stats["log_rows"] == 1
    assert stats["stored_success"] is True
    assert stats["late_failure_noop"] is True
    assert stats["log_unchanged"] is True
    assert stats["task_status"] == "done"
    assert stats["errors"] == []
