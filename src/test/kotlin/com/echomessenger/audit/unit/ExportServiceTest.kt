package com.echomessenger.audit.unit

import com.echomessenger.audit.domain.*
import com.echomessenger.audit.repository.ExportRepository
import com.echomessenger.audit.repository.MessageRepository
import com.echomessenger.audit.service.ExportService
import com.echomessenger.audit.service.UserNameResolver
import io.mockk.*
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.io.File

class ExportServiceTest {
    private val exportRepository: ExportRepository = mockk(relaxed = true)
    private val messageRepository: MessageRepository = mockk(relaxed = true)
    private val userNameResolver: UserNameResolver = mockk(relaxed = true)

    // Используем системный temp — не @TempDir, который JUnit удаляет до завершения coroutine
    private val tempDir =
        File(System.getProperty("java.io.tmpdir"), "audit-test-${System.nanoTime()}")
            .also { it.mkdirs() }

    private fun service() =
        ExportService(
            exportRepository = exportRepository,
            messageRepository = messageRepository,
            userNameResolver = userNameResolver,
            storageType = "pvc",
            pvcPath = tempDir.absolutePath,
        )

    @AfterEach
    fun cleanup() {
        // Небольшая задержка чтобы дать coroutine завершиться перед удалением
        Thread.sleep(200)
        tempDir.deleteRecursively()
    }

    // ── startExport ───────────────────────────────────────────────────────────

    @Test
    fun `startExport returns job in pending status immediately`() {
        val job = service().startExport(exportRequest())

        assertEquals(ExportStatus.pending, job.status)
        assertNotNull(job.exportId)
        assertTrue(job.exportId.isNotBlank())
        assertNull(job.downloadUrl, "downloadUrl should be null initially")
        assertNull(job.completedAt, "completedAt should be null initially")
    }

    @Test
    fun `startExport saves initial job to repository`() {
        service().startExport(exportRequest())

        verify { exportRepository.save(match { it.status == ExportStatus.pending }) }
    }

    @Test
    fun `startExport generates unique export IDs`() {
        val svc = service()
        val ids = (1..5).map { svc.startExport(exportRequest()).exportId }.toSet()

        assertEquals(5, ids.size, "All export IDs should be unique")
    }

    @Test
    fun `startExport respects requested format`() {
        val svc = service()
        assertEquals(ExportFormat.csv, svc.startExport(exportRequest(ExportFormat.csv)).format)
        assertEquals(ExportFormat.json, svc.startExport(exportRequest(ExportFormat.json)).format)
    }

    // ── getJob ────────────────────────────────────────────────────────────────

    @Test
    fun `getJob delegates to repository`() {
        val jobId = "test-export-id"
        val expected = completedJob(jobId)
        every { exportRepository.findById(jobId) } returns expected

        assertEquals(expected, service().getJob(jobId))
        verify { exportRepository.findById(jobId) }
    }

    @Test
    fun `getJob returns null for unknown id`() {
        every { exportRepository.findById(any()) } returns null
        assertNull(service().getJob("nonexistent"))
    }

    // ── async completion ──────────────────────────────────────────────────────

    @Test
    fun `startExport does not block caller — returns immediately`() {
        every { messageRepository.streamMessages(any(), any(), any()) } answers {
            Thread.sleep(500) // simula slow export
        }

        val start = System.currentTimeMillis()
        service().startExport(exportRequest())
        val elapsed = System.currentTimeMillis() - start

        assertTrue(elapsed < 300, "startExport should return immediately, took ${elapsed}ms")
    }

    @Test
    fun `startExport calls repository save at least once`() {
        every { messageRepository.streamMessages(any(), any(), any()) } just Runs

        service().startExport(exportRequest())

        verify(atLeast = 1) { exportRepository.save(any()) }
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private fun exportRequest(format: ExportFormat = ExportFormat.csv) =
        ExportRequest(
            filters =
                ExportFilters(
                    userId = "test-user",
                    fromTs = System.currentTimeMillis() - 86_400_000,
                    toTs = System.currentTimeMillis(),
                ),
            format = format,
        )

    private fun completedJob(id: String) =
        ExportJob(
            exportId = id,
            status = ExportStatus.completed,
            format = ExportFormat.csv,
            createdAt = System.currentTimeMillis() - 5000,
            completedAt = System.currentTimeMillis(),
            downloadUrl = "/api/v1/audit/export/$id/download",
            errorMessage = null,
            fileSizeBytes = 1024L,
        )
}
