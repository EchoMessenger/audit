package com.echomessenger.audit.api

import com.echomessenger.audit.config.RateLimitConfig
import com.echomessenger.audit.domain.ExportFilters
import com.echomessenger.audit.domain.ExportFormat
import com.echomessenger.audit.domain.ExportRequest
import com.echomessenger.audit.domain.MessageReportRequest
import com.echomessenger.audit.service.ExportService
import com.echomessenger.audit.service.ReportService
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.security.oauth2.jwt.Jwt
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/v1/audit/reports")
@PreAuthorize("hasAnyRole('audit_read', 'audit_admin')")
class ReportController(
    private val reportService: ReportService,
    private val exportService: ExportService,
    private val rateLimitConfig: RateLimitConfig,
) {
    /**
     * POST /api/v1/audit/reports/messages
     *
     * Rate limit: 5 запросов/минуту на токен.
     * Синхронно для периодов <= 7 дней.
     * Для периодов > 7 дней — 202 Accepted с redirect на async export.
     */
    @PostMapping("/messages")
    fun generateMessageReport(
        @RequestBody req: MessageReportRequest,
        @AuthenticationPrincipal jwt: Jwt,
    ): ResponseEntity<Any> {
        val userId = jwt.subject

        // Rate limit check
        val bucket = rateLimitConfig.getReportBucket(userId)
        if (!bucket.tryConsume(1)) {
            return ResponseEntity
                .status(HttpStatus.TOO_MANY_REQUESTS)
                .header("X-Rate-Limit-Retry-After-Seconds", "60")
                .body(mapOf("error" to "Rate limit exceeded: 5 requests/minute", "retryAfterSeconds" to 60))
        }

        // Если период > 7 дней — async export
        if (reportService.requiresAsyncExport(req)) {
            val exportReq =
                ExportRequest(
                    filters =
                        ExportFilters(
                            users = req.users,
                            topics = req.topics,
                            includeDeleted = req.includeDeleted,
                            fromTs = req.fromTs,
                            toTs = req.toTs,
                        ),
                    format = ExportFormat.json,
                )
            val job = exportService.startExport(exportReq)
            return ResponseEntity
                .status(HttpStatus.ACCEPTED)
                .body(
                    mapOf(
                        "message" to "Period exceeds 7 days — export started asynchronously",
                        "export_id" to job.exportId,
                        "status" to job.status,
                        "poll_url" to "/api/v1/audit/export/${job.exportId}",
                    ),
                )
        }

        val report = reportService.generateReport(req)
        return ResponseEntity.ok(report)
    }
}
