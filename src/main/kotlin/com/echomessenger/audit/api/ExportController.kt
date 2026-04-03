package com.echomessenger.audit.api

import com.echomessenger.audit.config.RateLimitConfig
import com.echomessenger.audit.domain.ExportJob
import com.echomessenger.audit.domain.ExportRequest
import com.echomessenger.audit.service.ExportService
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.io.FileSystemResource
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.security.oauth2.jwt.Jwt
import org.springframework.web.bind.annotation.*
import java.io.File
import java.net.URI

@RestController
@RequestMapping("/api/v1/audit/export")
@PreAuthorize("hasAnyRole('audit_read', 'audit_admin')")
class ExportController(
    private val exportService: ExportService,
    private val rateLimitConfig: RateLimitConfig,
    @Value("\${audit.export.pvc-path:/exports}") private val pvcPath: String,
) {
    /**
     * POST /api/v1/audit/export
     * Запускает async export. Возвращает job ID немедленно.
     */
    @PostMapping
    fun startExport(
        @RequestBody req: ExportRequest,
        @AuthenticationPrincipal jwt: Jwt,
    ): ResponseEntity<Map<String, Any>> {
        val userId = jwt.subject

        val bucket = rateLimitConfig.getExportBucket(userId)
        if (!bucket.tryConsume(1)) {
            return ResponseEntity
                .status(HttpStatus.TOO_MANY_REQUESTS)
                .header("X-Rate-Limit-Retry-After-Seconds", "60")
                .body(mapOf("error" to "Rate limit exceeded: 2 export requests/minute"))
        }

        val job = exportService.startExport(req)
        return ResponseEntity
            .status(HttpStatus.ACCEPTED)
            .body(
                mapOf(
                    "export_id" to job.exportId,
                    "status" to job.status.name,
                    "format" to job.format.name,
                    "created_at" to job.createdAt,
                ),
            )
    }

    /**
     * GET /api/v1/audit/export/{export_id}
     * Статус и download_url по завершении.
     */
    @GetMapping("/{exportId}")
    fun getExportStatus(
        @PathVariable exportId: String,
    ): ResponseEntity<ExportJob> {
        val job =
            exportService.getJob(exportId)
                ?: return ResponseEntity.notFound().build()
        return ResponseEntity.ok(job)
    }

    /**
     * GET /api/v1/audit/export/{export_id}/download
     * Прямая отдача файла с PVC (для storageType=pvc).
     * Для S3 — клиент идёт по presigned URL напрямую.
     */
    @GetMapping("/{exportId}/download")
    fun downloadExport(
        @PathVariable exportId: String,
    ): ResponseEntity<FileSystemResource> {
        val job =
            exportService.getJob(exportId)
                ?: return ResponseEntity.notFound().build()

        if (job.status.name != "completed") {
            return ResponseEntity.status(HttpStatus.ACCEPTED).build()
        }

        if (exportService.isS3Storage()) {
            if (!exportService.hasS3Object(exportId, job.format)) {
                return ResponseEntity.notFound().build()
            }
            val presignedUrl = exportService.generatePresignedDownloadUrl(exportId, job.format)
            return ResponseEntity.status(HttpStatus.FOUND)
                .location(URI.create(presignedUrl))
                .build()
        }

        val file = File("$pvcPath/$exportId.${job.format.name}")
        if (!file.exists()) return ResponseEntity.notFound().build()

        val mediaType =
            when (job.format.name) {
                "csv" -> "text/csv"
                "json" -> "application/json"
                else -> "application/octet-stream"
            }

        return ResponseEntity
            .ok()
            .contentType(MediaType.parseMediaType(mediaType))
            .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"audit-export-$exportId.${job.format.name}\"")
            .body(FileSystemResource(file))
    }
}
