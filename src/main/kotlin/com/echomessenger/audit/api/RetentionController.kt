package com.echomessenger.audit.api

import com.echomessenger.audit.domain.RetentionPolicy
import com.echomessenger.audit.repository.RetentionRepository
import org.springframework.http.ResponseEntity
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/v1/retention")
@PreAuthorize("hasAnyRole('audit_read', 'audit_admin')")
class RetentionController(
    private val retentionRepository: RetentionRepository,
) {
    /**
     * GET /api/v1/retention
     * Читает реальные TTL значения из system.tables ClickHouse.
     * POST /retention убран — изменение TTL управляется через Helm values и миграции.
     */
    @GetMapping
    fun getRetentionPolicies(): ResponseEntity<Map<String, List<RetentionPolicy>>> {
        val policies = retentionRepository.findRetentionPolicies()
        return ResponseEntity.ok(mapOf("policies" to policies))
    }
}
