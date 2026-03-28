package com.echomessenger.audit.service

import com.echomessenger.audit.repository.AnalyticsRepository
import com.echomessenger.audit.repository.SummaryResult
import com.echomessenger.audit.repository.TimeseriesPoint
import org.springframework.stereotype.Service

@Service
class AnalyticsService(
    private val analyticsRepository: AnalyticsRepository,
) {
    fun getSummary(
        fromTs: Long,
        toTs: Long,
    ): SummaryResult = analyticsRepository.getSummary(fromTs, toTs)

    fun getTimeseries(
        metric: String,
        interval: String,
        fromTs: Long,
        toTs: Long,
    ): List<TimeseriesPoint> {
        val allowedIntervals = setOf("hour", "day")
        require(interval in allowedIntervals) {
            "Unsupported interval '$interval'. Allowed: $allowedIntervals"
        }
        return analyticsRepository.getTimeseries(metric, interval, fromTs, toTs)
    }
}
