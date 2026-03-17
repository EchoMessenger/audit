-- src/test/resources/init-test-schema.sql
-- Инициализация схемы для Testcontainers ClickHouse

CREATE DATABASE IF NOT EXISTS audit;

-- client_req_log (упрощённая версия для тестов)
CREATE TABLE IF NOT EXISTS audit.client_req_log (
    log_id         UUID          DEFAULT generateUUIDv4(),
    req_ts         DateTime64(3) DEFAULT now64(3),
    msg_type       String,
    sess_user_id   String        DEFAULT '',
    sess_session_id String       DEFAULT '',
    sess_device_id String        DEFAULT '',
    sess_auth_level Int8         DEFAULT 0,
    client_ip      String        DEFAULT '',
    user_agent     String        DEFAULT ''
) ENGINE = MergeTree()
ORDER BY (req_ts, log_id);

-- message_log
CREATE TABLE IF NOT EXISTS audit.message_log (
    seq_id         Int64         DEFAULT 0,
    msg_ts         DateTime64(3) DEFAULT now64(3),
    msg_type       String,
    usr_id         String        DEFAULT '',
    topic_id       String        DEFAULT '',
    content        String        DEFAULT ''
) ENGINE = MergeTree()
ORDER BY (msg_ts, seq_id);

-- account_log
CREATE TABLE IF NOT EXISTS audit.account_log (
    user_id        String,
    display_name   String        DEFAULT '',
    updated_at     DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (user_id);

-- subscription_log
CREATE TABLE IF NOT EXISTS audit.subscription_log (
    event_ts       DateTime64(3) DEFAULT now64(3),
    user_id        String        DEFAULT '',
    topic_id       String        DEFAULT '',
    action         String        DEFAULT '',
    new_role       String        DEFAULT '',
    old_role       String        DEFAULT ''
) ENGINE = MergeTree()
ORDER BY (event_ts, user_id);

-- incident_log
CREATE TABLE IF NOT EXISTS audit.incident_log (
    incident_id    UUID          DEFAULT generateUUIDv4(),
    detected_at    DateTime64(3) DEFAULT now64(3),
    type           String,
    status         String        DEFAULT 'open',
    user_id        String        DEFAULT '',
    details        String        DEFAULT '{}',
    updated_at     DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(detected_at)
ORDER BY (incident_id);

-- export_job_log
CREATE TABLE IF NOT EXISTS audit.export_job_log (
    export_id      UUID          DEFAULT generateUUIDv4(),
    status         String        DEFAULT 'pending',
    format         String        DEFAULT 'csv',
    created_at     DateTime64(3) DEFAULT now64(3),
    completed_at   DateTime64(3) DEFAULT toDateTime64(0, 3),
    download_url   String        DEFAULT '',
    error_message  String        DEFAULT '',
    file_size_bytes Int64        DEFAULT 0
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (export_id);

-- Materialized views (упрощённые для тестов)
CREATE TABLE IF NOT EXISTS audit.mv_daily_msg_type_stats (
    day            Date,
    msg_type       String,
    cnt            Int64
) ENGINE = SummingMergeTree()
ORDER BY (day, msg_type);

CREATE TABLE IF NOT EXISTS audit.mv_daily_user_activity (
    day            Date,
    usr_id         String,
    event_count    Int64
) ENGINE = SummingMergeTree()
ORDER BY (day, usr_id);

CREATE TABLE IF NOT EXISTS audit.mv_hourly_load_stats (
    hour_ts        DateTime,
    msg_type       String,
    event_count    Int64
) ENGINE = SummingMergeTree()
ORDER BY (hour_ts, msg_type);

CREATE TABLE IF NOT EXISTS audit.mv_daily_message_stats (
    day            Date,
    msg_type       String,
    cnt            Int64
) ENGINE = SummingMergeTree()
ORDER BY (day, msg_type);
