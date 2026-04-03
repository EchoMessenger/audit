package com.echomessenger.audit.repository

import com.echomessenger.audit.domain.ExportFormat
import com.echomessenger.audit.domain.ExportJob
import com.echomessenger.audit.domain.ExportStatus
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository

@Repository
class ExportRepository(
    @Qualifier("clickHouseJdbcTemplate")
    private val jdbc: NamedParameterJdbcTemplate,
) {
    fun save(job: ExportJob) {
        val rowVersion = nextRowVersion(job.exportId)
        jdbc.update(
            """
            INSERT INTO audit.export_job_log
                (export_id, status, format, created_at, completed_at, download_url, error_message, file_size_bytes, row_version)
            VALUES
                (toUUID(:exportId), :status, :format,
                 fromUnixTimestamp64Milli(:createdAt),
                 ${if (job.completedAt != null) "fromUnixTimestamp64Milli(:completedAt)" else "toDateTime64(0, 3)"},
                 :downloadUrl, :errorMessage, :fileSizeBytes, :rowVersion)
            """.trimIndent(),
            MapSqlParameterSource().apply {
                addValue("exportId", job.exportId)
                addValue("status", job.status.name)
                addValue("format", job.format.name)
                addValue("createdAt", job.createdAt)
                addValue("completedAt", job.completedAt)
                addValue("downloadUrl", job.downloadUrl)
                addValue("errorMessage", job.errorMessage)
                addValue("fileSizeBytes", job.fileSizeBytes)
                addValue("rowVersion", rowVersion)
            },
        )
    }

    fun findById(exportId: String): ExportJob? {
        if (!isValidUuid(exportId)) return null
        return runCatching {
            jdbc
                .query(
                    """
                    SELECT export_id, status, format,
                           toUnixTimestamp64Milli(created_at)   AS created_at,
                           toUnixTimestamp64Milli(completed_at) AS completed_at,
                           download_url, error_message, file_size_bytes
                    FROM audit.export_job_log
                    WHERE export_id = toUUID(:exportId)
                    ORDER BY row_version DESC
                    LIMIT 1
                    """.trimIndent(),
                    MapSqlParameterSource("exportId", exportId),
                ) { rs, _ ->
                    ExportJob(
                        exportId = rs.getString("export_id"),
                        status = ExportStatus.valueOf(rs.getString("status")),
                        format = ExportFormat.valueOf(rs.getString("format")),
                        createdAt = rs.getLong("created_at"),
                        completedAt = rs.getLong("completed_at").takeIf { it > 0 },
                        downloadUrl = rs.getString("download_url")?.takeIf { it.isNotBlank() },
                        errorMessage = rs.getString("error_message")?.takeIf { it.isNotBlank() },
                        fileSizeBytes = rs.getLong("file_size_bytes").takeIf { it > 0 },
                    )
                }.firstOrNull()
        }.getOrNull()
    }

    private fun nextRowVersion(exportId: String): Long {
        val current =
            jdbc.queryForObject(
                """
                SELECT coalesce(max(row_version), 0)
                FROM audit.export_job_log
                WHERE export_id = toUUID(:exportId)
                """.trimIndent(),
                MapSqlParameterSource("exportId", exportId),
                Long::class.java,
            ) ?: 0L
        return maxOf(current + 1, System.currentTimeMillis())
    }

    private fun isValidUuid(value: String): Boolean = runCatching { java.util.UUID.fromString(value) }.isSuccess
}
