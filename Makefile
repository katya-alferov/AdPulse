# Shortcuts. On Windows without "make", copy the command after the colon line.
.PHONY: init up down reset ps logs test test-spark test-integration batch demo-export demo

init:            ## create .env from the example (edit the passwords afterwards)
	test -f .env || cp .env.example .env

up: init         ## build and start the whole platform
	docker compose up -d --build

down:            ## stop everything (data is kept)
	docker compose down

reset:           ## stop everything AND delete all data volumes
	docker compose down -v

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=100

test:            ## fast unit tests (no Docker needed)
	python -m pytest tests/unit -q

test-spark:      ## PySpark tests inside the Spark container
	docker compose run --rm --no-deps spark-streaming python3 -m pytest tests/spark -q -p no:cacheprovider

test-integration: ## end-to-end tests against the running stack
	python -m pytest -m integration tests/integration -v

batch:           ## run the Spark batch report (publisher rankings)
	docker compose run --rm spark-batch

demo-export:     ## export a synthetic snapshot for the public demo
	python tools/export_demo_data.py --api http://localhost:8000 --out demo_data

demo:            ## run the dashboard in demo mode (no Docker needed)
	streamlit run streamlit_app.py
