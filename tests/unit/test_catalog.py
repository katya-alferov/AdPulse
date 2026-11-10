"""The Python catalog and the SQL seed file must describe the same rows."""

import re
from pathlib import Path

from adpulse.generator.catalog import CAMPAIGNS, PUBLISHERS

SEED = Path(__file__).resolve().parents[2] / "sql" / "init" / "02_seed_dimensions.sql"


def test_seed_matches_catalog():
    sql = SEED.read_text()
    campaign_ids = set(re.findall(r"\('(cmp_\d+)'", sql))
    publisher_ids = set(re.findall(r"\('(pub_\d+)'", sql))
    assert campaign_ids == {c.campaign_id for c in CAMPAIGNS}
    assert publisher_ids == {p.publisher_id for p in PUBLISHERS}


def test_ids_unique():
    assert len({c.campaign_id for c in CAMPAIGNS}) == len(CAMPAIGNS)
    assert len({p.publisher_id for p in PUBLISHERS}) == len(PUBLISHERS)
