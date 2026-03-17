-- V1__create_audit_service_tables.sql
-- Новые таблицы для audit-service. Основные таблицы (client_req_log, message_log и т.д.)
-- уже созданы ingestor'ом — здесь только новые объекты для audit-service.

-- ── incident_log ──────────────────────────────────────────────────────────────
-- ReplacingMergeTree по updated_at позволяет обновлять статус инцидента
-- через INSERT новой версии строки с тем же incident_id.
CREATE TABLE IF NOT EXISTS audit.incident_log (
    incident_id   UUID          DEFAULT generateUUIDv4(),
    detected_at   DateTime64(3) DEFAULT now64(3),
    type          String,
    status        String        DEFAULT 'open',
    user_id       String        DEFAULT '',
    details       String        DEFAULT '{}',  -- JSON
    updated_at    DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(detected_at)
ORDER BY (incident_id)
SETTINGS index_granularity = 8192;

-- Индекс для быстрого поиска по статусу и типу
ALTER TABLE audit.incident_log
    ADD INDEX IF NOT EXISTS idx_status (status) TYPE set(10) GRANULARITY 1;

ALTER TABLE audit.incident_log
    ADD INDEX IF NOT EXISTS idx_type (type) TYPE set(20) GRANULARITY 1;

ALTER TABLE audit.incident_log
    ADD INDEX IF NOT EXISTS idx_user_id (user_id) TYPE bloom_filter(0.01) GRANULARITY 1;

-- ── export_job_log ────────────────────────────────────────────────────────────
-- ReplacingMergeTree для хранения статуса export job с возможностью обновления.
CREATE TABLE IF NOT EXISTS audit.export_job_log (
    export_id       UUID          DEFAULT generateUUIDv4(),
    status          String        DEFAULT 'pending',   -- pending | running | completed | failed
    format          String        DEFAULT 'csv',       -- csv | json
    created_at      DateTime64(3) DEFAULT now64(3),
    completed_at    DateTime64(3) DEFAULT toDateTime64(0, 3),
    download_url    String        DEFAULT '',
    error_message   String        DEFAULT '',
    file_size_bytes Int64         DEFAULT 0
) ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (export_id)
SETTINGS index_granularity = 8192;
