-- Certified Finance only. Never sum sales and scheduled_settlement together.
CREATE OR REPLACE VIEW lakehouse.analytics.marketplace_finance_daily AS
SELECT * FROM lakehouse.analytics.mart_commerce_finance_daily
WHERE report_type='sales' AND channel NOT LIKE 'direct_%';

CREATE OR REPLACE VIEW lakehouse.analytics.marketplace_settlement_schedule AS
SELECT * FROM lakehouse.analytics.mart_commerce_finance_daily
WHERE report_type='scheduled_settlement';

CREATE OR REPLACE VIEW lakehouse.analytics.comparable_merchandise_daily AS
SELECT business_date,channel,comparable_merchandise,eligible_orders,units_sold,aov_merchandise,
       coverage_status,marketplace_day_complete,source_observed_at,publication_id,data_as_of,published_at
FROM lakehouse.analytics.mart_commerce_finance_daily
WHERE report_type='sales';
