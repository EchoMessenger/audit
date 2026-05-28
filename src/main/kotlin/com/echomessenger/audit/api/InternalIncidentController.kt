package com.echomessenger.audit.api

import com.echomessenger.audit.service.IncidentService
import org.springframework.http.ResponseEntity
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/internal/incidents")
class InternalIncidentController(
    private val incidentService: IncidentService,
) {
    @PostMapping("/detect")
    @PreAuthorize("hasRole('audit_admin')")
    fun triggerDetection(): ResponseEntity<Map<String, Any>> {
        incidentService.runDetection()
        return ResponseEntity.ok(
            mapOf(
                "status" to "ok",
                "trigger" to "manual",
                "timestamp" to System.currentTimeMillis(),
            ),
        )
    }
}
