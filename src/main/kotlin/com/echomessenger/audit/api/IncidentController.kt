package com.echomessenger.audit.api

import com.echomessenger.audit.domain.Incident
import com.echomessenger.audit.domain.IncidentDetail
import com.echomessenger.audit.domain.UpdateIncidentStatusRequest
import com.echomessenger.audit.service.AuditService
import com.echomessenger.audit.service.IncidentService
import org.springframework.http.ResponseEntity
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/v1/incidents")
class IncidentController(
    private val incidentService: IncidentService,
    private val auditService: AuditService,
) {
    /**
     * GET /api/v1/incidents
     * Список инцидентов с фильтрацией.
     */
    @GetMapping
    @PreAuthorize("hasAnyRole('audit_read', 'audit_admin')")
    fun listIncidents(
        @RequestParam(required = false) status: String?,
        @RequestParam(required = false) type: String?,
        @RequestParam(required = false) userId: String?,
        @RequestParam(defaultValue = "100") limit: Int,
    ): ResponseEntity<Map<String, List<Incident>>> {
        val incidents =
            incidentService.listIncidents(
                status = status,
                type = type,
                userId = userId,
                limit = limit.coerceIn(1, 1000),
            )
        return ResponseEntity.ok(mapOf("incidents" to incidents))
    }

    /**
     * GET /api/v1/incidents/{incident_id}
     * Детализация инцидента с событиями из client_req_log.
     */
    @GetMapping("/{incidentId}")
    @PreAuthorize("hasAnyRole('audit_read', 'audit_admin')")
    fun getIncident(
        @PathVariable incidentId: String,
    ): ResponseEntity<IncidentDetail> {
        val incident =
            incidentService.getIncident(incidentId)
                ?: return ResponseEntity.notFound().build()

        // Достаём связанные события пользователя вокруг времени инцидента
        val windowMs = 5 * 60 * 1000L // ±5 минут от времени обнаружения
        val events =
            if (incident.userId != null) {
                auditService
                    .findEvents(
                        userId = incident.userId,
                        fromTs = incident.detectedAt - windowMs,
                        toTs = incident.detectedAt + windowMs,
                        limit = 50,
                        actorUserId = null,
                        topicId = null,
                        eventType = null,
                        status = null,
                        cursor = null,
                    ).data
            } else {
                emptyList()
            }

        return ResponseEntity.ok(IncidentDetail(incident = incident, events = events))
    }

    /**
     * POST /api/v1/incidents/{incident_id}/status
     * Обновление статуса инцидента — только audit_admin.
     */
    @PostMapping("/{incidentId}/status")
    @PreAuthorize("hasRole('audit_admin')")
    fun updateStatus(
        @PathVariable incidentId: String,
        @RequestBody req: UpdateIncidentStatusRequest,
    ): ResponseEntity<Incident> =
        try {
            val updated =
                incidentService.updateStatus(
                    incidentId = incidentId,
                    status = req.status,
                    comment = req.comment,
                )
            ResponseEntity.ok(updated)
        } catch (e: NoSuchElementException) {
            ResponseEntity.notFound().build()
        } catch (e: IllegalArgumentException) {
            ResponseEntity.badRequest().build()
        }
}
