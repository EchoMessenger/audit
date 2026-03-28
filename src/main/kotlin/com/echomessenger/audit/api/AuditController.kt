package com.echomessenger.audit.api

import com.echomessenger.audit.domain.AuditEvent
import com.echomessenger.audit.domain.CursorPage
import com.echomessenger.audit.domain.SessionSummary
import com.echomessenger.audit.service.AuditService
import org.springframework.http.ResponseEntity
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/v1/audit")
@PreAuthorize("hasAnyRole('audit_read', 'audit_admin')")
class AuditController(
    private val auditService: AuditService,
) {
    /**
     * GET /api/v1/audit/events
     * Поиск и фильтрация событий из всех таблиц.
     */
    @GetMapping("/events")
    fun getEvents(
        @RequestParam(required = false) userId: String?,
        @RequestParam(required = false) actorUserId: String?,
        @RequestParam(required = false) topicId: String?,
        @RequestParam(required = false) eventType: String?,
        @RequestParam(required = false) fromTs: Long?,
        @RequestParam(required = false) toTs: Long?,
        @RequestParam(required = false) status: String?,
        @RequestParam(required = false) cursor: String?,
        @RequestParam(defaultValue = "100") limit: Int,
    ): ResponseEntity<CursorPage<AuditEvent>> {
        if (fromTs != null && toTs != null) {
            require(toTs > fromTs) { "toTs must be greater than fromTs" }
        }
        val page =
            auditService.findEvents(
                userId = userId,
                actorUserId = actorUserId,
                topicId = topicId,
                eventType = eventType,
                fromTs = fromTs,
                toTs = toTs,
                status = status,
                cursor = cursor,
                limit = limit.coerceIn(1, 1000),
            )
        return ResponseEntity.ok(page)
    }

    /**
     * GET /api/v1/audit/events/{event_id}
     */
    @GetMapping("/events/{eventId}")
    fun getEvent(
        @PathVariable eventId: String,
    ): ResponseEntity<Map<String, AuditEvent>> {
        val event =
            auditService.findEventById(eventId)
                ?: return ResponseEntity.notFound().build()
        return ResponseEntity.ok(mapOf("event" to event))
    }

    /**
     * GET /api/v1/audit/auth-events
     * Только LOGIN, HI, BYE, REG события.
     */
    @GetMapping("/auth-events")
    fun getAuthEvents(
        @RequestParam(required = false) userId: String?,
        @RequestParam(required = false) fromTs: Long?,
        @RequestParam(required = false) toTs: Long?,
        @RequestParam(required = false) cursor: String?,
        @RequestParam(defaultValue = "100") limit: Int,
    ): ResponseEntity<CursorPage<AuditEvent>> {
        val page =
            auditService.findAuthEvents(
                userId = userId,
                fromTs = fromTs,
                toTs = toTs,
                cursor = cursor,
                limit = limit.coerceIn(1, 1000),
            )
        return ResponseEntity.ok(page)
    }

    /**
     * GET /api/v1/audit/sessions
     * Сессионная аналитика, сгруппированная по sess_session_id.
     */
    @GetMapping("/sessions")
    fun getSessions(
        @RequestParam(required = false) userId: String?,
        @RequestParam(required = false) fromTs: Long?,
        @RequestParam(required = false) toTs: Long?,
        @RequestParam(required = false) cursor: String?,
        @RequestParam(defaultValue = "50") limit: Int,
    ): ResponseEntity<CursorPage<SessionSummary>> {
        val page =
            auditService.findSessions(
                userId = userId,
                fromTs = fromTs,
                toTs = toTs,
                cursor = cursor,
                limit = limit.coerceIn(1, 500),
            )
        return ResponseEntity.ok(page)
    }
}
