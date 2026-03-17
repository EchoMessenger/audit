package com.echomessenger.audit.integration

import com.echomessenger.audit.domain.*
import com.echomessenger.audit.repository.ExportRepository
import com.echomessenger.audit.repository.MessageRepository
import com.echomessenger.audit.service.ExportService
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import java.nio.file.Path
import java.util.UUID
import kotlin.io.path.listDirectoryEntries

class ExportServiceIT : IntegrationTestBase() {
    @Autowired private lateinit var exportRepository: ExportRepository

    @Autowired private lateinit var messageRepository: MessageRepository

    @Autowired private lateinit var jdbc: NamedParameterJdbcTemplate

    @TempDir
    lateinit var tempDir: Path

    // Создаём ExportService с TempDir — обходим @Value pvcPath из контекста
    private fun service() =
        ExportService(
            exportRepository = exportRepository,
            messageRepository = messageRepository,
            storageType = "pvc",
            pvcPath = tempDir.toString(),
        )

    // ── CSV export ────────────────────────────────────────────────────────────

    @Test
    fun `CSV export creates file and reaches completed status`() {
        val userId = "exp_csv_${UUID.randomUUID().toString().take(8)}"
        seedMessages(userId)

        val job =
            service().startExport(
                ExportRequest(
                    filters =
                        ExportFilters(
                            userId = userId,
                            fromTs = System.currentTimeMillis() - 3_600_000,
                            toTs = System.currentTimeMillis(),
                        ),
                    format = ExportFormat.csv,
                ),
            )

        assertEquals(ExportStatus.pending, job.status, "Should start as pending")

        // Ждём завершения coroutine (Dispatchers.IO)
        Thread.sleep(3000)

        val completed = exportRepository.findById(job.exportId)
        assertNotNull(completed, "Job should be persisted in export_job_log")
        assertEquals(
            ExportStatus.completed,
            completed!!.status,
            "Job should reach completed, got: ${completed.status} error: ${completed.errorMessage}",
        )
        assertNotNull(completed.completedAt, "completedAt should be set")
        assertNotNull(completed.downloadUrl, "downloadUrl should be set")
        assertTrue((completed.fileSizeBytes ?: 0) > 0, "File size should be > 0")

        // Файл существует на диске
        val files = tempDir.listDirectoryEntries("${job.exportId}.csv")
        assertTrue(files.isNotEmpty(), "CSV file should exist in $tempDir")
        val content = files.first().toFile().readText()
        assertTrue(content.startsWith("message_id,"), "CSV should have header row")
        assertTrue(content.lines().size > 1, "CSV should have data rows")
    }

    // ── JSON export ───────────────────────────────────────────────────────────

    @Test
    fun `JSON export creates valid JSON array file`() {
        val userId = "exp_json_${UUID.randomUUID().toString().take(8)}"
        seedMessages(userId)

        val job =
            service().startExport(
                ExportRequest(
                    filters =
                        ExportFilters(
                            userId = userId,
                            fromTs = System.currentTimeMillis() - 3_600_000,
                            toTs = System.currentTimeMillis(),
                        ),
                    format = ExportFormat.json,
                ),
            )

        Thread.sleep(3000)

        val files = tempDir.listDirectoryEntries("${job.exportId}.json")
        assertTrue(files.isNotEmpty(), "JSON file should exist")
        val content =
            files
                .first()
                .toFile()
                .readText()
                .trim()
        assertTrue(content.startsWith("["), "JSON export should start with '['")
        assertTrue(content.endsWith("]"), "JSON export should end with ']'")
    }

    // ── empty result ──────────────────────────────────────────────────────────

    @Test
    fun `export with no matching data completes successfully with empty file`() {
        val job =
            service().startExport(
                ExportRequest(
                    filters =
                        ExportFilters(
                            userId = "nonexistent_${UUID.randomUUID()}",
                            fromTs = System.currentTimeMillis() - 3_600_000,
                            toTs = System.currentTimeMillis(),
                        ),
                    format = ExportFormat.csv,
                ),
            )

        Thread.sleep(3000)

        val completed = exportRepository.findById(job.exportId)
        assertEquals(
            ExportStatus.completed,
            completed?.status,
            "Empty export should complete, not fail",
        )
    }

    // ── getJob ────────────────────────────────────────────────────────────────

    @Test
    fun `getJob returns null for unknown export id`() {
        val result = service().getJob("completely-unknown-id")
        assertNull(result)
    }

    @Test
    fun `getJob returns persisted job`() {
        val job =
            service().startExport(
                ExportRequest(
                    filters =
                        ExportFilters(
                            fromTs = System.currentTimeMillis() - 3600_000,
                            toTs = System.currentTimeMillis(),
                        ),
                    format = ExportFormat.csv,
                ),
            )

        // Сразу после startExport — должно быть в БД в статусе pending или running
        val found = service().getJob(job.exportId)
        assertNotNull(found, "Job should be findable immediately after startExport")
        assertEquals(job.exportId, found!!.exportId)
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private fun seedMessages(userId: String) {
        val base = System.currentTimeMillis() * 100 + userId.hashCode().and(0xFFFF).toLong()
        jdbc.update(
            """
            INSERT INTO audit.message_log
                (seq_id, msg_ts, msg_type, usr_id, topic_id, content)
            VALUES
                (:s1, now64(3) - INTERVAL 30 MINUTE, 'PUB',  :u, 'topic_exp', 'Export msg 1'),
                (:s2, now64(3) - INTERVAL 20 MINUTE, 'PUB',  :u, 'topic_exp', 'Export msg 2'),
                (:s3, now64(3) - INTERVAL 10 MINUTE, 'EDIT', :u, 'topic_exp', 'Edited msg')
            """.trimIndent(),
            MapSqlParameterSource("u", userId)
                .addValue("s1", base + 1)
                .addValue("s2", base + 2)
                .addValue("s3", base + 3),
        )
        Thread.sleep(500)
    }
}
