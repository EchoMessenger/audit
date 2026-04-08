-- ReplacingMergeTree(created_at) не уникализирует обновления job с тем же created_at.
-- Пересоздаём таблицу с row_version (монотонно растёт при каждом INSERT из приложения).

CREATE TABLE IF NOT EXISTS audit.export_job_log_new (
    export_id       UUID,
    status          String,
    format          String,
    created_at      DateTime64(3),
    completed_at    DateTime64(3),
    download_url    String,
    error_message   String,
    file_size_bytes Int64,
    row_version     UInt64
) ENGINE = ReplacingMergeTree(row_version)
PARTITION BY toYYYYMM(created_at)
ORDER BY (export_id)
SETTINGS index_granularity = 8192;

INSERT INTO audit.export_job_log_new
SELECT
    export_id,
    argMax(
        status,
        (
            multiIf(status = 'running', 1, status = 'pending', 0, 2),
            toUnixTimestamp64Milli(completed_at),
            toUnixTimestamp64Milli(created_at)
        )
    ) AS status,
    argMax(
        format,
        (
            multiIf(status = 'running', 1, status = 'pending', 0, 2),
            toUnixTimestamp64Milli(completed_at),
            toUnixTimestamp64Milli(created_at)
        )
    ) AS format,
    min(created_at) AS created_at,
    argMax(
        completed_at,
        (
            multiIf(status = 'running', 1, status = 'pending', 0, 2),
            toUnixTimestamp64Milli(completed_at),
            toUnixTimestamp64Milli(created_at)
        )
    ) AS completed_at,
    argMax(
        download_url,
        (
            multiIf(status = 'running', 1, status = 'pending', 0, 2),
            toUnixTimestamp64Milli(completed_at),
            toUnixTimestamp64Milli(created_at)
        )
    ) AS download_url,
    argMax(
        error_message,
        (
            multiIf(status = 'running', 1, status = 'pending', 0, 2),
            toUnixTimestamp64Milli(completed_at),
            toUnixTimestamp64Milli(created_at)
        )
    ) AS error_message,
    argMax(
        file_size_bytes,
        (
            multiIf(status = 'running', 1, status = 'pending', 0, 2),
            toUnixTimestamp64Milli(completed_at),
            toUnixTimestamp64Milli(created_at)
        )
    ) AS file_size_bytes,
    toUInt64(toUnixTimestamp64Milli(now64(3))) AS row_version
FROM audit.export_job_log
GROUP BY export_id;

RENAME TABLE audit.export_job_log TO audit.export_job_log_old,
             audit.export_job_log_new TO audit.export_job_log;

DROP TABLE audit.export_job_log_old;
