-- =====================================================================
-- AdPulse PostgreSQL schema
-- Runs automatically the first time the postgres container starts
-- (files in /docker-entrypoint-initdb.d are executed in alphabetical order).
-- =====================================================================

-- ---------- Dimension tables (descriptive data) ----------
CREATE TABLE IF NOT EXISTS campaigns (
    campaign_id       VARCHAR(20)  PRIMARY KEY,
    campaign_name     VARCHAR(100) NOT NULL,
    advertiser        VARCHAR(100) NOT NULL,
    category          VARCHAR(50)  NOT NULL,
    daily_budget_usd  NUMERIC(12,2) NOT NULL CHECK (daily_budget_usd >= 0),
    cpc_usd           NUMERIC(8,4)  NOT NULL CHECK (cpc_usd >= 0)
);

CREATE TABLE IF NOT EXISTS publishers (
    publisher_id    VARCHAR(20)  PRIMARY KEY,
    publisher_name  VARCHAR(100) NOT NULL,
    site_category   VARCHAR(50)  NOT NULL,
    country         CHAR(2)      NOT NULL
);

-- ---------- Fact table: metrics per 1-minute window (written by Spark) ----------
CREATE TABLE IF NOT EXISTS metrics_minute (
    window_start   TIMESTAMPTZ  NOT NULL,
    window_end     TIMESTAMPTZ  NOT NULL,
    campaign_id    VARCHAR(20)  NOT NULL REFERENCES campaigns(campaign_id),
    publisher_id   VARCHAR(20)  NOT NULL REFERENCES publishers(publisher_id),
    impressions    BIGINT       NOT NULL DEFAULT 0,
    clicks         BIGINT       NOT NULL DEFAULT 0,
    conversions    BIGINT       NOT NULL DEFAULT 0,
    spend_usd      NUMERIC(14,4) NOT NULL DEFAULT 0,
    revenue_usd    NUMERIC(14,2) NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, campaign_id, publisher_id)
);
CREATE INDEX IF NOT EXISTS idx_metrics_campaign ON metrics_minute (campaign_id, window_start);
CREATE INDEX IF NOT EXISTS idx_metrics_publisher ON metrics_minute (publisher_id, window_start);

-- ---------- Fraud signals: too many clicks from one user+IP in one minute ----------
CREATE TABLE IF NOT EXISTS suspicious_activity (
    window_start   TIMESTAMPTZ NOT NULL,
    window_end     TIMESTAMPTZ NOT NULL,
    user_id        VARCHAR(50) NOT NULL,
    ip_address     VARCHAR(45) NOT NULL,
    campaign_id    VARCHAR(20) NOT NULL,
    click_count    BIGINT      NOT NULL,
    reason         VARCHAR(100) NOT NULL,
    detected_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, user_id, ip_address, campaign_id)
);

-- ---------- Duplicate events (same event_id received more than once) ----------
CREATE TABLE IF NOT EXISTS duplicate_events (
    event_id       VARCHAR(64) PRIMARY KEY,
    event_type     VARCHAR(20) NOT NULL,
    campaign_id    VARCHAR(20) NOT NULL,
    user_id        VARCHAR(50) NOT NULL,
    occurrences    BIGINT      NOT NULL,
    detected_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------- Batch job output: publisher ranking inside each campaign ----------
CREATE TABLE IF NOT EXISTS publisher_rankings (
    campaign_id       VARCHAR(20) NOT NULL REFERENCES campaigns(campaign_id),
    publisher_id      VARCHAR(20) NOT NULL REFERENCES publishers(publisher_id),
    impressions       BIGINT      NOT NULL,
    clicks            BIGINT      NOT NULL,
    conversions       BIGINT      NOT NULL,
    ctr_pct           NUMERIC(7,3) NOT NULL,
    cvr_pct           NUMERIC(7,3) NOT NULL,
    rank_in_campaign  INT         NOT NULL,
    computed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (campaign_id, publisher_id)
);

-- ---------- Invalid events counter per minute (written by Spark) ----------
CREATE TABLE IF NOT EXISTS invalid_events_minute (
    window_start      TIMESTAMPTZ NOT NULL,
    validation_error  VARCHAR(60) NOT NULL,
    event_count       BIGINT      NOT NULL,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, validation_error)
);

-- ---------- Reporting views (SQL the API reads) ----------
CREATE OR REPLACE VIEW v_campaign_performance AS
SELECT c.campaign_id,
       c.campaign_name,
       c.advertiser,
       c.category,
       COALESCE(SUM(m.impressions), 0)  AS impressions,
       COALESCE(SUM(m.clicks), 0)       AS clicks,
       COALESCE(SUM(m.conversions), 0)  AS conversions,
       COALESCE(SUM(m.spend_usd), 0)    AS spend_usd,
       COALESCE(SUM(m.revenue_usd), 0)  AS revenue_usd,
       ROUND(100.0 * COALESCE(SUM(m.clicks), 0) / NULLIF(SUM(m.impressions), 0), 2) AS ctr_pct,
       ROUND(100.0 * COALESCE(SUM(m.conversions), 0) / NULLIF(SUM(m.clicks), 0), 2) AS cvr_pct
FROM campaigns c
LEFT JOIN metrics_minute m ON m.campaign_id = c.campaign_id
GROUP BY c.campaign_id, c.campaign_name, c.advertiser, c.category;

CREATE OR REPLACE VIEW v_publisher_performance AS
SELECT p.publisher_id,
       p.publisher_name,
       p.site_category,
       p.country,
       COALESCE(SUM(m.impressions), 0)  AS impressions,
       COALESCE(SUM(m.clicks), 0)       AS clicks,
       COALESCE(SUM(m.conversions), 0)  AS conversions,
       COALESCE(SUM(m.spend_usd), 0)    AS spend_usd,
       ROUND(100.0 * COALESCE(SUM(m.clicks), 0) / NULLIF(SUM(m.impressions), 0), 2) AS ctr_pct,
       ROUND(100.0 * COALESCE(SUM(m.conversions), 0) / NULLIF(SUM(m.clicks), 0), 2) AS cvr_pct
FROM publishers p
LEFT JOIN metrics_minute m ON m.publisher_id = p.publisher_id
GROUP BY p.publisher_id, p.publisher_name, p.site_category, p.country;
