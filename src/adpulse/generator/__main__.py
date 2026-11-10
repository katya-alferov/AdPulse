"""Run the event generator:  python -m adpulse.generator

Reads EVENTS_PER_SECOND / GENERATOR_DURATION_SECONDS / anomaly rates from env.
"""

from __future__ import annotations

import logging
import signal
import sys
import time
from typing import Any

from adpulse.config import Settings
from adpulse.generator.event_factory import EventFactory
from adpulse.kafka_io.admin import ensure_topics
from adpulse.kafka_io.producer import AdEventProducer
from adpulse.logging_setup import setup_logging

log = logging.getLogger("adpulse.generator")


class GeneratorRunner:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.factory = EventFactory(settings.generator)
        self.producer = AdEventProducer(settings.kafka.bootstrap_servers, settings.kafka.events_topic)
        self._running = True

    def stop(self, *_: Any) -> None:
        self._running = False

    def run(self) -> None:
        cfg = self.settings.generator
        interval = 1.0 / cfg.events_per_second  # time between impressions
        started = time.monotonic()
        last_report = started
        log.info("Generating ~%.1f impressions/s to topic %s", cfg.events_per_second, self.producer.topic)
        while self._running:
            tick = time.monotonic()
            for payload in self.factory.next_batch():
                self.producer.send(payload)
            if tick - last_report >= 10:
                s = self.factory.stats
                log.info(
                    "sent=%d delivered=%d failed=%d | imp=%d clk=%d conv=%d dup=%d invalid=%d late=%d bot_clicks=%d",
                    s.total, self.producer.delivered, self.producer.failed, s.impressions, s.clicks,
                    s.conversions, s.duplicates, s.invalid, s.late, s.bot_clicks,
                )
                last_report = tick
            if cfg.duration_seconds and tick - started >= cfg.duration_seconds:
                break
            sleep_for = interval - (time.monotonic() - tick)
            if sleep_for > 0:
                time.sleep(sleep_for)
        remaining = self.producer.flush()
        log.info("Generator finished. stats=%s still_queued=%d", self.factory.stats, remaining)


def main() -> int:
    settings = Settings.from_env()
    setup_logging(settings.log_level)
    ensure_topics(settings.kafka)
    runner = GeneratorRunner(settings)
    signal.signal(signal.SIGTERM, runner.stop)
    signal.signal(signal.SIGINT, runner.stop)
    runner.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
