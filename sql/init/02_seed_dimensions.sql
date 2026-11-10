-- Synthetic dimension data (fictional). Must match src/adpulse/generator/catalog.py
INSERT INTO campaigns (campaign_id, campaign_name, advertiser, category, daily_budget_usd, cpc_usd) VALUES
    ('cmp_001', 'Summer Sneakers Sale', 'StrideWear', 'fashion', 500.00, 0.4500),
    ('cmp_002', 'Budget Flights to Europe', 'SkyHop Travel', 'travel', 800.00, 0.7000),
    ('cmp_003', 'Learn Python Online', 'CodeNest Academy', 'education', 300.00, 0.3500),
    ('cmp_004', 'Smart Home Starter Kit', 'HomeByte', 'electronics', 650.00, 0.5500),
    ('cmp_005', 'Healthy Meal Boxes', 'GreenFork', 'food', 400.00, 0.4000),
    ('cmp_006', 'Personal Finance App', 'CoinWise', 'finance', 700.00, 0.8000),
    ('cmp_007', 'Mystery Novel Release', 'PageTurner Books', 'books', 200.00, 0.2500),
    ('cmp_008', 'Electric Scooter Launch', 'VoltRide', 'mobility', 900.00, 0.6500)
ON CONFLICT (campaign_id) DO NOTHING;

INSERT INTO publishers (publisher_id, publisher_name, site_category, country) VALUES
    ('pub_01', 'DailyNews Hub', 'news', 'US'),
    ('pub_02', 'TechTalk Online', 'technology', 'US'),
    ('pub_03', 'Foodie Planet', 'food', 'GB'),
    ('pub_04', 'TravelMore Magazine', 'travel', 'DE'),
    ('pub_05', 'SportZone Live', 'sports', 'IL'),
    ('pub_06', 'FinanceToday', 'finance', 'US')
ON CONFLICT (publisher_id) DO NOTHING;
