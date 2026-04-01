package com.echomessenger.audit.integration

import com.echomessenger.audit.domain.*
import com.echomessenger.audit.repository.ExportRepository
import com.echomessenger.audit.repository.MessageRepository
import com.echomessenger.audit.service.ExportService
import com.echomessenger.audit.service.UserNameResolver
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import java.nio.file.Path
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.UUID
import kotlin.io.path.listDirectoryEntries

private fun chTs(instant: Instant): String =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
        .withZone(ZoneOffset.UTC)
        .format(instant)

class ExportServiceIT : IntegrationTestBase() {
    @Autowired private lateinit var exportRepository: ExportRepository
    @Autowired private lateinit var messageRepository: MessageRepository
    @Autowired private lateinit var jdbc: NamedParameterJdbcTemplate
    private val userNameResolver: UserNameResolver = io.mockk.mockk(relaxed = true)

    @TempDir lateinit var tempDir: Path

    private fun service() = ExportService(
        exportRepository = exportRepository,
        messageRepository = messageRepository,
        userNameResolver = userNameResolver,
        storageType = "pvc",
        pvcPath = tempDir.toString(),
    )

    @Test
    fun `CSV export creates file and reaches completed status`() {
        val userId = "exp_csv_${UUID.randomUUID().toString().take(8)}"
        seedMessages(userId)

        val job = service().startExport(
            ExportRequest(
                filters = ExportFilters(
                    userId = userId,
                    fromTs = System.currentTimeMillis() - 3_600_000,
                    toTs = System.currentTimeMillis(),
                ),
                format = ExportFormat.csv,
            ),
        )
        assertEquals(ExportStatus.pending, job.status)

        Thread.sleep(3000)

        val completed = (1..20).asSequence()
            .map { Thread.sleep(500); exportRepository.findById(job.exportId) }
            .firstOrNull { it?.status == ExportStatus.completed || it?.status == ExportStatus.failed }

        assertNotNull(completed)
        assertEquals(ExportStatus.completed, completed!!.status,
            "got: ${completed.status}, error: ${completed.errorMessage}")
        assertNotNull(completed.completedAt)
        assertNotNull(completed.downloadUrl)
        assertTrue((completed.fileSizeBytes ?: 0) > 0)

        val files = tempDir.listDirectoryEntries("${job.exportId}.csv")
        assertTrue(files.isNotEmpty())
        val content = files.first().toFile().readText()
        assertTrue(content.startsWith("message_id,"))
        assertTrue(content.lines().size > 1)
    }

    @Test
    fun `JSON export creates valid JSON array file`() {
        val userId = "exp_json_${UUID.randomUUID().toString().take(8)}"
        seedMessages(userId)

        val job = service().startExport(
            ExportRequest(
                filters = ExportFilters(
                    userId = userId,
                    fromTs = System.currentTimeMillis() - 3_600_000,
                    toTs = System.currentTimeMillis(),
                ),
                format = ExportFormat.json,
            ),
        )

        Thread.sleep(3000)

        val files = tempDir.listDirectoryEntries("${job.exportId}.json")
        assertTrue(files.isNotEmpty())
        val content = files.first().toFile().readText().trim()
        assertTrue(content.startsWith("["))
        assertTrue(content.endsWith("]"))
    }

    @Test
    fun `export with no matching data completes successfully with empty file`() {
        val job = service().startExport(
            ExportRequest(
                filters = ExportFilters(
                    userId = "nonexistent_${UUID.randomUUID()}",
                    fromTs = System.currentTimeMillis() - 3_600_000,
                    toTs = System.currentTimeMillis(),
                ),
                format = ExportFormat.csv,
            ),
        )

        Thread.sleep(3000)

        val completed = exportRepository.findById(job.exportId)
        assertEquals(ExportStatus.completed, completed?.status)
    }

    @Test
    fun `getJob returns null for unknown export id`() {
        assertNull(service().getJob("completely-unknown-id"))
    }

    @Test
    fun `getJob returns persisted job`() {
        val job = service().startExport(
            ExportRequest(
                filters = ExportFilters(
                    fromTs = System.currentTimeMillis() - 3600_000,
                    toTs = System.currentTimeMillis(),
                ),
                format = ExportFormat.csv,
            ),
        )
        val found = service().getJob(job.exportId)
        assertNotNull(found)
        assertEquals(job.exportId, found!!.exportId)
    }

    private fun seedMessages(userId: String) {
        val now = Instant.now()
        listOf(
            Triple(1800L, "CREATE", 1),
            Triple(1200L, "CREATE", 2),
            Triple(600L,  "UPDATE", 1),
        ).forEach { (offsetSec, action, seqId) ->
            jdbc.update(
                """INSERT INTO audit.message_log
                   (log_id, log_timestamp, action, msg_topic, msg_from_user_id,
                    msg_timestamp, msg_seq_id, msg_content)
                   VALUES (:id, :ts, :act, :topic, :uid, :msgTs, :seqId, :content)""",
                MapSqlParameterSource()
                    .addValue("id", UUID.randomUUID().toString())
                    .addValue("ts", chTs(now.minusSeconds(offsetSec)))
                    .addValue("act", action)
                    .addValue("topic", "topic_exp")
                    .addValue("uid", userId)
                    .addValue("msgTs", now.minusSeconds(offsetSec).toEpochMilli())
                    .addValue("seqId", seqId)
                    .addValue("content", "Export msg $seqId"),
            )
        }
        Thread.sleep(500)
    }
}