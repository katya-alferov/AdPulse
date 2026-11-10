"""Entry point for the PUBLIC demo (e.g. Streamlit Community Cloud).

It starts the same dashboard in demo mode: data comes from the synthetic
snapshot in demo_data/ instead of the live Kafka/Spark/PostgreSQL backend.
For the full system run `docker compose up -d --build` (see README).
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
os.environ.setdefault("ADPULSE_MODE", "demo")

import runpy  # noqa: E402

runpy.run_path(str(Path(__file__).resolve().parent / "src" / "adpulse" / "dashboard" / "app.py"), run_name="__main__")
