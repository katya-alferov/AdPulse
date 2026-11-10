"""Write a small file of example events (what flows through Kafka).

    python tools/make_sample_events.py > demo_data/sample_events.jsonl
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from adpulse.config import GeneratorSettings  # noqa: E402
from adpulse.generator.event_factory import EventFactory  # noqa: E402
from adpulse.models import AdEvent  # noqa: E402

factory = EventFactory(GeneratorSettings(seed=2026, duplicate_rate=0.03, invalid_rate=0.03, late_event_rate=0.03,
                                         bot_burst_probability=0.01))
now = datetime(2026, 11, 10, 12, 0, tzinfo=timezone.utc)
count = 0
while count < 300:
    for item in factory.next_batch(now):
        line = item.to_json() if isinstance(item, AdEvent) else (item if isinstance(item, str) else json.dumps(item))
        print(line)
        count += 1
