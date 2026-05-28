package com.echomessenger.audit.unit

import com.echomessenger.audit.api.ExportController
import com.echomessenger.audit.config.RateLimitConfig
import com.echomessenger.audit.domain.ExportFormat
import com.echomessenger.audit.domain.ExportJob
import com.echomessenger.audit.domain.ExportRequest
import com.echomessenger.audit.domain.ExportStatus
import com.echomessenger.audit.service.ExportService
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.springframework.http.HttpStatus
import org.springframework.security.oauth2.jwt.Jwt
import java.nio.file.Path
import kotlin.io.path.writeText

class ExportControllerTest {
    private val exportService: ExportService = mockk(relaxed = true)
    private val jwt: Jwt = mockk {
        every { subject } returns "export-controller-user"
    }

    @TempDir
    lateinit var tempDir: Path

    @Test
    fun `startExport returns accepted then rate limits same user`() {
        val controller = controller(exportPerMinute = 2)
        every { exportService.startExport(any()) } returns job("export-1", ExportStatus.pending)

        val first = controller.startExport(request(), jwt)
        val second = controller.startExport(request(), jwt)
        val third = controller.startExport(request(), jwt)

        assertEquals(HttpStatus.ACCEPTED, first.statusCode)
        assertEquals(HttpStatus.ACCEPTED, second.statusCode)
        assertEquals(HttpStatus.TOO_MANY_REQUESTS, third.statusCode)
        assertEquals("60", third.headers.getFirst("X-Rate-Limit-Retry-After-Seconds"))
        verify(exactly = 2) { exportService.startExport(any()) }
    }

    @Test
    fun `downloadExport returns status branches for missing and pending jobs`() {
        val controller = controller()

        every { exportService.getJob("missing") } returns null
        assertEquals(HttpStatus.NOT_FOUND, controller.downloadExport("missing").statusCode)

        every { exportService.getJob("pending") } returns job("pending", ExportStatus.running)
        assertEquals(HttpStatus.ACCEPTED, controller.downloadExport("pending").statusCode)
    }

    @Test
    fun `downloadExport redirects to presigned S3 URL when object exists`() {
        val controller = controller()
        every { exportService.getJob("s3-export") } returns job("s3-export", ExportStatus.completed)
        every { exportService.isS3Storage() } returns true
        every { exportService.hasS3Object("s3-export", ExportFormat.csv) } returns true
        every { exportService.generatePresignedDownloadUrl("s3-export", ExportFormat.csv) } returns
            "https://storage.example/presigned"

        val response = controller.downloadExport("s3-export")

        assertEquals(HttpStatus.FOUND, response.statusCode)
        assertEquals("https://storage.example/presigned", response.headers.location.toString())
    }

    @Test
    fun `downloadExport returns not found for missing S3 object`() {
        val controller = controller()
        every { exportService.getJob("s3-missing") } returns job("s3-missing", ExportStatus.completed)
        every { exportService.isS3Storage() } returns true
        every { exportService.hasS3Object("s3-missing", ExportFormat.csv) } returns false

        assertEquals(HttpStatus.NOT_FOUND, controller.downloadExport("s3-missing").statusCode)
    }

    @Test
    fun `downloadExport returns PVC file with content disposition`() {
        val exportId = "pvc-export"
        tempDir.resolve("$exportId.csv").writeText("message_id\n1\n")
        val controller = controller()
        every { exportService.getJob(exportId) } returns job(exportId, ExportStatus.completed)
        every { exportService.isS3Storage() } returns false

        val response = controller.downloadExport(exportId)

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals("text/csv", response.headers.contentType.toString())
        assertTrue(response.headers.contentDisposition.filename!!.contains(exportId))
        assertNotNull(response.body)
    }

    private fun controller(exportPerMinute: Long = 10) =
        ExportController(
            exportService = exportService,
            rateLimitConfig = RateLimitConfig(reportsPerMinute = 10, exportPerMinute = exportPerMinute),
            pvcPath = tempDir.toString(),
        )

    private fun request() = ExportRequest(filters = com.echomessenger.audit.domain.ExportFilters(), format = ExportFormat.csv)

    private fun job(
        exportId: String,
        status: ExportStatus,
    ) = ExportJob(
        exportId = exportId,
        status = status,
        format = ExportFormat.csv,
        createdAt = 1L,
        completedAt = if (status == ExportStatus.completed) 2L else null,
        downloadUrl = if (status == ExportStatus.completed) "/api/v1/audit/export/$exportId/download" else null,
        errorMessage = null,
        fileSizeBytes = if (status == ExportStatus.completed) 12L else null,
    )
}
