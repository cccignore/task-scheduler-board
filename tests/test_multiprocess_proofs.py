import multiprocessing.process

import pytest

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
    assert stats["ready_processes"] == 6
    assert stats["ready_connections"] == 6
    assert len(stats["ready_connection_records"]) == 6
    assert stats["ready_connections_verified"] is True
    assert stats["claim_attempts"] == 72
    assert stats["observed_results"] == 72
    assert stats["result_pair_anomalies"] == 0
    assert stats["unique_winners"] == 12
    assert stats["duplicate_claims"] == 0
    assert stats["anomalous_rounds"] == 0
    assert stats["claim_metadata_anomalies"] == 0
    assert stats["errors"] == []
    assert stats["claimed_tasks"] == 12
    assert stats["pending_tasks"] == 0
    assert stats["passed"] is True


def test_five_spawn_processes_write_one_immutable_success_log(database_path):
    stats = run_idempotency_proof(database_path, processes=5, quiet=True)

    assert stats["start_method"] == "spawn"
    assert stats["processes"] == 5
    assert stats["ready_processes"] == 5
    assert stats["ready_connections"] == 5
    assert len(stats["ready_connection_records"]) == 5
    assert stats["ready_connections_verified"] is True
    assert stats["completion_calls"] == 5
    assert stats["observed_results"] == 5
    assert stats["inserted_responses"] == 1
    assert stats["duplicate_responses"] == 4
    assert stats["invalid_response_pairs"] == 0
    assert stats["response_pattern_exact"] is True
    assert stats["log_rows"] == 1
    assert stats["stored_success"] is True
    assert stats["late_failure_noop"] is True
    assert stats["row_unchanged"] is True
    assert stats["failure_first_noop"] is True
    assert stats["task_status"] == "done"
    assert stats["errors"] == []
    assert stats["passed"] is True


def test_partial_start_failure_is_preserved_and_only_started_processes_are_cleaned(
    tmp_path, monkeypatch
):
    original_start = multiprocessing.process.BaseProcess.start

    def fail_on_second_start():
        calls = 0

        def patched_start(process):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("synthetic process start failure")
            return original_start(process)

        return patched_start

    monkeypatch.setattr(
        multiprocessing.process.BaseProcess, "start", fail_on_second_start()
    )

    with pytest.raises(RuntimeError, match="synthetic process start failure"):
        run_claim_proof(
            tmp_path / "claim-start-failure.db",
            rounds=1,
            workers=2,
            quiet=True,
        )
    assert not any(
        child.name.startswith("claim-proof-")
        for child in multiprocessing.active_children()
    )

    monkeypatch.setattr(
        multiprocessing.process.BaseProcess, "start", fail_on_second_start()
    )
    with pytest.raises(RuntimeError, match="synthetic process start failure"):
        run_idempotency_proof(
            tmp_path / "idempotency-start-failure.db",
            processes=5,
            quiet=True,
        )
    assert not any(
        child.name.startswith("idempotency-proof-")
        for child in multiprocessing.active_children()
    )
