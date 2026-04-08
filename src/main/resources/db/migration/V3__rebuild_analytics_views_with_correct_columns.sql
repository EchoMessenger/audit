-- V3__rebuild_analytics_views_with_correct_columns.sql
--
-- Purpose:
-- - Keep V2 immutable.
-- - Rebuild analytics objects using columns that exist in current tables.
-- - Use store tables + writer materialized views + read views.
--
-- Notes:
-- - This migration is intended for environments where SQL migrations are applied manually
--   or Flyway is enabled in the future.
-- - Existing canonical analytics objects are recreated as VIEW aliases over *_store tables.

-- 1) Store tables
CREATE TABLE IF NOT EXISTS audit.mv_daily_msg_type_stats_store (
    day      Date,
    msg_type String,
    cnt      UInt64
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (day, msg_type);

CREATE TABLE IF NOT EXISTS audit.mv_daily_user_activity_store (
    day         Date,
    usr_id      String,
    event_count UInt64
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (day, usr_id);

CREATE TABLE IF NOT EXISTS audit.mv_hourly_load_stats_store (
    hour_ts     DateTime,
    msg_type    String,
    event_count UInt64
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour_ts)
ORDER BY (hour_ts, msg_type);

CREATE TABLE IF NOT EXISTS audit.mv_daily_message_stats_store (
    day      Date,
    msg_type String,
    cnt      UInt64
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (day, msg_type);

-- 2) Writers (attach to stores; POPULATE removed for atomic backfill control)
CREATE MATERIALIZED VIEW IF NOT EXISTS audit.mv_daily_msg_type_stats_writer
TO audit.mv_daily_msg_type_stats_store
AS
SELECT
    toDate(log_timestamp) AS day,
    msg_type,
    count() AS cnt
FROM audit.client_req_log
GROUP BY day, msg_type;

CREATE MATERIALIZED VIEW IF NOT EXISTS audit.mv_daily_user_activity_writer
TO audit.mv_daily_user_activity_store
AS
SELECT
    toDate(log_timestamp) AS day,
    sess_user_id AS usr_id,
    count() AS event_count
FROM audit.client_req_log
WHERE sess_user_id != ''
GROUP BY day, usr_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS audit.mv_hourly_load_stats_writer
TO audit.mv_hourly_load_stats_store
AS
SELECT
    toStartOfHour(log_timestamp) AS hour_ts,
    msg_type,
    count() AS event_count
FROM audit.client_req_log
GROUP BY hour_ts, msg_type;

CREATE MATERIALIZED VIEW IF NOT EXISTS audit.mv_daily_message_stats_writer
TO audit.mv_daily_message_stats_store
AS
SELECT
    toDate(log_timestamp) AS day,
    multiIf(
        toString(action) = 'CREATE', 'PUB',
        toString(action) = 'UPDATE', 'EDIT',
        toString(action) = 'DELETE', 'DEL',
        toString(action)
    ) AS msg_type,
    count() AS cnt
FROM audit.message_log
GROUP BY day, msg_type;

-- 3) Canonical read aliases
-- Drop old objects with the same names (could be VIEW or MATERIALIZED VIEW in old setups).
-- Use DROP TABLE to remove both the view and any implicit internal storage from old materialized views.
DROP TABLE IF EXISTS audit.mv_daily_msg_type_stats;
DROP TABLE IF EXISTS audit.mv_daily_user_activity;
DROP TABLE IF EXISTS audit.mv_hourly_load_stats;
DROP TABLE IF EXISTS audit.mv_daily_message_stats;

CREATE VIEW IF NOT EXISTS audit.mv_daily_msg_type_stats AS
SELECT
    day,
    msg_type,
    sum(cnt) AS cnt
FROM audit.mv_daily_msg_type_stats_store
GROUP BY day, msg_type;

CREATE VIEW IF NOT EXISTS audit.mv_daily_user_activity AS
SELECT
    day,
    usr_id,
    sum(event_count) AS event_count
FROM audit.mv_daily_user_activity_store
GROUP BY day, usr_id;

CREATE VIEW IF NOT EXISTS audit.mv_hourly_load_stats AS
SELECT
    hour_ts,
    msg_type,
    sum(event_count) AS event_count
FROM audit.mv_hourly_load_stats_store
GROUP BY hour_ts, msg_type;

CREATE VIEW IF NOT EXISTS audit.mv_daily_message_stats AS
SELECT
    day,
    msg_type,
    sum(cnt) AS cnt
FROM audit.mv_daily_message_stats_store
GROUP BY day, msg_type;
