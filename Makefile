.PHONY: install run test proof

install:
	python3 -m pip install -e '.[dev]'

run:
	python3 -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000

test:
	python3 -m pytest

proof:
	python3 scripts/run_concurrency_proof.py
	python3 scripts/run_idempotency_proof.py

