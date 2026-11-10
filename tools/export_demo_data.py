"""Export a snapshot of the live pipeline's results for the public demo.

Run while the full stack is up (after it has processed data for a while):

    python tools/export_demo_data.py --api http://localhost:8000 --out demo_data

Only aggregated, synthetic analytics are exported - no credentials, no
configuration, no raw infrastructure details.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from adpulse.dashboard.api_client import AdPulseClient  # noqa: E402
from adpulse.generator.catalog import CAMPAIGNS  # noqa: E402


def dump(path: Path, data) -> None:
    path.write_text(json.dumps(data, indent=1, default=str), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api", default="http://localhost:8000")
    parser.add_argument("--out", default="demo_data")
    parser.add_argument("--minutes", type=int, default=720)
    args = parser.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    client = AdPulseClient(args.api, timeout=30)

    dump(out / "summary.json", client.summary())
    dump(out / "campaigns.json", client.campaigns())
    dump(out / "publishers.json", client.publishers())
    dump(out / "timeseries.json", client.timeseries(args.minutes))
    dump(out / "timeseries_by_campaign.json",
         {c.campaign_id: client.timeseries(args.minutes, c.campaign_id) for c in CAMPAIGNS})
    dump(out / "suspicious.json", client.suspicious(200))
    dump(out / "duplicates.json", client.duplicates(200))
    dump(out / "invalid.json", client.invalid())
    dump(out / "rankings.json", client.rankings())
    dump(out / "metadata.json", {
        "exported_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": "Local AdPulse run: generator -> Kafka -> Spark Structured Streaming -> PostgreSQL -> FastAPI",
        "data": "100% synthetic (fictional campaigns, publishers and users)",
    })
    print(f"Demo snapshot written to {out.resolve()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
