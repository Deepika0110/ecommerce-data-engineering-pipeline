setup:
	python -m venv .venv
	source .venv/bin/activate && pip install -r requirements.txt

run:
	python scripts/run_pipeline.py

checks:
	python scripts/run_checks.py

docker:
	docker compose up --build

clean:
	rm -rf __pycache__ .pytest_cache


