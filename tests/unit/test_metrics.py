from adpulse.metrics import conversion_rate, cpc, ctr, roas, safe_ratio_percent


def test_ctr_basic():
    assert ctr(clicks=25, impressions=1000) == 2.5


def test_ctr_zero_impressions_is_zero_not_error():
    assert ctr(5, 0) == 0.0


def test_conversion_rate():
    assert conversion_rate(conversions=3, clicks=40) == 7.5


def test_rounding():
    assert safe_ratio_percent(1, 3) == 33.33


def test_cpc_and_roas():
    assert cpc(10.0, 4) == 2.5
    assert cpc(10.0, 0) == 0.0
    assert roas(300.0, 100.0) == 3.0
    assert roas(10.0, 0) == 0.0
