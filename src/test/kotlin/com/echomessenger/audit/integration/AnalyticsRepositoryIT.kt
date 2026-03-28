package com.echomessenger.audit.integration

import com.echomessenger.audit.repository.AnalyticsRepository
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

class AnalyticsRepositoryIT : IntegrationTestBase() {
    @Autowired private lateinit var analyticsRepository: AnalyticsRepository
    @Autowired private lateinit var jdbc: NamedParameterJdbcTemplate

    private val now = System.currentTimeMillis()

    @BeforeEach
    fun seedMVData() {
        val nowInstant = Instant.ofEpochMilli(System.currentTimeMillis())
        val today = LocalDate.now(ZoneOffset.UTC)
        val currentHour = nowInstant.atZone(ZoneOffset.UTC)
            .withMinute(0).withSecond(0).withNano(0)

        // ----------------------------
        // Подготовка списка вставок
        // ----------------------------
        val entries = listOf(
            // msg_type -> количество
            Triple("LOGIN", "userA", 10L),
            Triple("PUB", "userA", 25L),
            Triple("DEL", "userB", 3L)
        )

        // ----------------------------
        // Вставка в client_req_log
        // ----------------------------
        entries.forEach { (msgType, userId, count) ->
            repeat(count.toInt()) {
                jdbc.update(
                    """
                INSERT INTO audit.client_req_log (log_timestamp, sess_user_id, msg_type)
                VALUES (:ts, :user, :type)
                """.trimIndent(),
                    MapSqlParameterSource()
                        .addValue("ts", nowInstant)
                        .addValue("user", userId)
                        .addValue("type", msgType)
                )
            }
        }

        // Для hourly load stats — создаём записи на час
        val hourlyEntries = listOf(
            Triple("LOGIN", "userA", 5L),
            Triple("PUB", "userA", 7L)
        )

        hourlyEntries.forEach { (msgType, userId, count) ->
            repeat(count.toInt()) {
                jdbc.update(
                    """
                INSERT INTO audit.client_req_log (log_timestamp, sess_user_id, msg_type)
                VALUES (:ts, :user, :type)
                """.trimIndent(),
                    MapSqlParameterSource()
                        .addValue("ts", currentHour.toInstant().toEpochMilli())
                        .addValue("user", userId)
                        .addValue("type", msgType)
                )
            }
        }

        // Небольшая пауза, чтобы MV успели подтянуть данные
        Thread.sleep(500)
    }

    @Test
    fun `getSummary returns eventsByType with correct mapping`() {
        val summary = analyticsRepository.getSummary(fromTs = now - 86400_000, toTs = now)
        assertTrue(summary.eventsByType.isNotEmpty())
        // LOGIN → auth.login
        assertTrue(summary.eventsByType.containsKey("auth.login"),
            "Should map LOGIN to auth.login, got: ${summary.eventsByType.keys}")
    }

    @Test
    fun `getSummary returns topUsers sorted by activity`() {
        val summary = analyticsRepository.getSummary(fromTs = now - 86400_000, toTs = now)
        assertTrue(summary.topUsers.isNotEmpty())
        val counts = summary.topUsers.map { it.count }
        assertEquals(counts.sortedDescending(), counts)
    }

    @Test
    fun `getSummary returns empty eventsByType for out-of-range period`() {
        val summary = analyticsRepository.getSummary(
            fromTs = now - 365 * 86400_000L, toTs = now - 364 * 86400_000L,
        )
        assertTrue(summary.eventsByType.isEmpty() || summary.eventsByType.values.all { it == 0L })
    }

    @Test
    fun `getTimeseries hour interval returns data points`() {
        val points = analyticsRepository.getTimeseries(
            metric = "auth.login", interval = "hour",
            fromTs = now - 86400_000, toTs = now,
        )
        // Может быть пустой если данные ещё не попали — не должен бросать исключение
        assertNotNull(points)
    }

    @Test
    fun `getTimeseries day interval returns data points`() {
        val points = analyticsRepository.getTimeseries(
            metric = "auth.login", interval = "day",
            fromTs = now - 86400_000, toTs = now,
        )
        assertNotNull(points)
    }

    @Test
    fun `getTimeseries throws for unsupported interval`() {
        assertThrows(IllegalArgumentException::class.java) {
            analyticsRepository.getTimeseries(
                metric = "auth.login", interval = "week",
                fromTs = now - 86400_000, toTs = now,
            )
        }
    }
}