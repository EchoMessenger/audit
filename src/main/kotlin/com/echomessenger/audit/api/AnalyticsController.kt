package com.echomessenger.audit.api

import com.echomessenger.audit.repository.SummaryResult
import com.echomessenger.audit.repository.TimeseriesPoint
import com.echomessenger.audit.service.AnalyticsService
import org.springframework.http.ResponseEntity
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/v1/analytics")
@PreAuthorize("hasAnyRole('audit_read', 'audit_admin')")
class AnalyticsController(
    private val analyticsService: AnalyticsService,
) {
    /**
     * GET /api/v1/analytics/summary
     * Агрегированные показатели из materialized views.
     * Кэш 5 минут — не требует realtime точности.
     */
    @GetMapping("/summary")
    fun getSummary(
        @RequestParam fromTs: Long,
        @RequestParam toTs: Long,
    ): ResponseEntity<SummaryResult> {
        require(toTs > fromTs) { "toTs must be greater than fromTs" }
        return ResponseEntity.ok(analyticsService.getSummary(fromTs, toTs))
    }

    /**
     * GET /api/v1/analytics/timeseries
     * Временной ряд по конкретной метрике.
     * interval: "hour" | "day"
     * metric: "message.create" | "auth.login" | "auth.register" | "topic.create"
     */
    @GetMapping("/timeseries")
    fun getTimeseries(
        @RequestParam metric: String,
        @RequestParam(defaultValue = "hour") interval: String,
        @RequestParam fromTs: Long,
        @RequestParam toTs: Long,
    ): ResponseEntity<Map<String, Any>> {
        require(toTs > fromTs) { "toTs must be greater than fromTs" }
        val points = analyticsService.getTimeseries(metric, interval, fromTs, toTs)
        return ResponseEntity.ok(
            mapOf(
                "metric" to metric,
                "interval" to interval,
                "from_ts" to fromTs,
                "to_ts" to toTs,
                "points" to points,
            ),
        )
    }
}
