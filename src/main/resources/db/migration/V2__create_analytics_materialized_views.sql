-- V2__create_analytics_materialized_views.sql
-- Materialized views для analytics endpoints.
-- AnalyticsRepository читает только из этих view — не сканирует сырые таблицы.

-- ── mv_daily_msg_type_stats ───────────────────────────────────────────────────
-- Агрегирует количество событий по типу за день из client_req_log.
CREATE MATERIALIZED VIEW IF NOT EXISTS audit.mv_daily_msg_type_stats
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (day, msg_type)
POPULATE
AS
SELECT
    toDate(log_timestamp) AS day,
    msg_type,
    count()               AS cnt
FROM audit.client_req_log
GROUP BY day, msg_type;

-- ── mv_daily_user_activity ────────────────────────────────────────────────────
-- Активность пользователей по дням для top_users в summary.
CREATE MATERIALIZED VIEW IF NOT EXISTS audit.mv_daily_user_activity
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (day, usr_id)
POPULATE
AS
SELECT
    toDate(log_timestamp) AS day,
    sess_user_id          AS usr_id,
    count()               AS event_count
FROM audit.client_req_log
WHERE sess_user_id != ''
GROUP BY day, usr_id;

-- ── mv_hourly_load_stats ──────────────────────────────────────────────────────
-- Почасовая нагрузка для timeseries с interval=hour.
CREATE MATERIALIZED VIEW IF NOT EXISTS audit.mv_hourly_load_stats
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour_ts)
ORDER BY (hour_ts, msg_type)
POPULATE
AS
SELECT
    toStartOfHour(log_timestamp) AS hour_ts,
    msg_type,
    count()                     AS event_count
FROM audit.client_req_log
GROUP BY hour_ts, msg_type;

-- ── mv_daily_message_stats ────────────────────────────────────────────────────
-- Ежедневные сообщения для timeseries с interval=day (по message_log).
CREATE MATERIALIZED VIEW IF NOT EXISTS audit.mv_daily_message_stats
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (day, msg_type)
POPULATE
AS
SELECT
    toDate(log_timestamp) AS day,
    multiIf(
        toString(action) = 'CREATE', 'PUB',
        toString(action) = 'UPDATE', 'EDIT',
        toString(action) = 'DELETE', 'DEL',
        toString(action)
    ) AS msg_type,
    count()               AS cnt
FROM audit.message_log
GROUP BY day, msg_type;
