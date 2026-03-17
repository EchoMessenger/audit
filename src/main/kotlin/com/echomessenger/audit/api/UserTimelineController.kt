package com.echomessenger.audit.api

import com.echomessenger.audit.domain.AuditEvent
import com.echomessenger.audit.domain.CursorPage
import com.echomessenger.audit.service.AuditService
import org.springframework.http.ResponseEntity
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/v1/audit/users")
@PreAuthorize("hasAnyRole('audit_read', 'audit_admin')")
class UserTimelineController(
    private val auditService: AuditService,
) {
    /**
     * GET /api/v1/audit/users/{user_id}/timeline
     * Полная хронологическая история пользователя:
     * client_req_log + message_log + subscription_log.
     */
    @GetMapping("/{userId}/timeline")
    fun getTimeline(
        @PathVariable userId: String,
        @RequestParam(required = false) fromTs: Long?,
        @RequestParam(required = false) toTs: Long?,
        @RequestParam(required = false) cursor: String?,
        @RequestParam(defaultValue = "100") limit: Int,
    ): ResponseEntity<CursorPage<AuditEvent>> {
        val page =
            auditService.getUserTimeline(
                userId = userId,
                fromTs = fromTs,
                toTs = toTs,
                cursor = cursor,
                limit = limit.coerceIn(1, 500),
            )
        return ResponseEntity.ok(page)
    }
}
