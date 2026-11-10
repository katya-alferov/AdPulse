import pytest

from adpulse.config import ConfigError, Settings


def test_defaults(monkeypatch):
    for var in ("POSTGRES_HOST", "EVENTS_PER_SECOND", "GENERATOR_SEED"):
        monkeypatch.delenv(var, raising=False)
    s = Settings.from_env()
    assert s.postgres.host == "localhost"
    assert s.generator.events_per_second == 40.0
    assert s.generator.seed is None


def test_env_overrides(monkeypatch):
    monkeypatch.setenv("POSTGRES_HOST", "db")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret123")
    monkeypatch.setenv("EVENTS_PER_SECOND", "5")
    monkeypatch.setenv("GENERATOR_SEED", "42")
    s = Settings.from_env()
    assert s.postgres.host == "db"
    assert s.generator.events_per_second == 5.0
    assert s.generator.seed == 42


def test_password_is_not_printed(monkeypatch):
    monkeypatch.setenv("POSTGRES_PASSWORD", "super-secret")
    monkeypatch.setenv("S3_SECRET_KEY", "s3-secret")
    s = Settings.from_env()
    assert "super-secret" not in repr(s)
    assert "s3-secret" not in repr(s)


def test_invalid_number(monkeypatch):
    monkeypatch.setenv("EVENTS_PER_SECOND", "fast")
    with pytest.raises(ConfigError):
        Settings.from_env()


def test_rate_out_of_range(monkeypatch):
    monkeypatch.setenv("DUPLICATE_RATE", "1.5")
    with pytest.raises(ConfigError):
        Settings.from_env()
