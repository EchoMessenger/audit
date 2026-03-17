package com.echomessenger.audit.config

import com.github.benmanes.caffeine.cache.Caffeine
import io.github.bucket4j.Bandwidth
import io.github.bucket4j.Bucket
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Configuration
import java.time.Duration
import java.util.concurrent.TimeUnit

@Configuration
class RateLimitConfig(
    @Value("\${audit.rate-limit.reports-per-minute:5}") private val reportsPerMinute: Long,
    @Value("\${audit.rate-limit.export-per-minute:2}") private val exportPerMinute: Long,
) {
    // Caffeine кэш bucket-ов по userId — вычищается через 1 час неактивности
    private val reportBuckets =
        Caffeine
            .newBuilder()
            .expireAfterAccess(1, TimeUnit.HOURS)
            .build<String, Bucket>()

    private val exportBuckets =
        Caffeine
            .newBuilder()
            .expireAfterAccess(1, TimeUnit.HOURS)
            .build<String, Bucket>()

    fun getReportBucket(userId: String): Bucket = reportBuckets.get(userId) { createBucket(reportsPerMinute) }!!

    fun getExportBucket(userId: String): Bucket = exportBuckets.get(userId) { createBucket(exportPerMinute) }!!

    private fun createBucket(requestsPerMinute: Long): Bucket =
        Bucket
            .builder()
            .addLimit(
                Bandwidth
                    .builder()
                    .capacity(requestsPerMinute)
                    .refillGreedy(requestsPerMinute, Duration.ofMinutes(1))
                    .build(),
            ).build()
}
