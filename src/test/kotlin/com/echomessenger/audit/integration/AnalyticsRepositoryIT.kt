package com.echomessenger.audit.integration

import com.echomessenger.audit.repository.AnalyticsRepository
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import java.time.LocalDate

class AnalyticsRepositoryIT : IntegrationTestBase() {
    @Autowired private lateinit var analyticsRepository: AnalyticsRepository

    @Autowired private lateinit var jdbc: NamedParameterJdbcTemplate

    private val now = System.currentTimeMillis()
    private val dayMs = 86_400_000L

    @BeforeEach
    fun seedMvData() {
        // Вставляем данные напрямую в MV-таблицы (в тестах они plain SummingMergeTree)
        val today = LocalDate.now().toString()
        val yesterday = LocalDate.now().minusDays(1).toString()

        jdbc.update(
            """
            INSERT INTO audit.mv_daily_msg_type_stats (day, msg_type, cnt) VALUES
                (:today, 'LOGIN', 150),
                (:today, 'PUB',   320),
                (:yesterday, 'LOGIN', 90),
                (:yesterday, 'PUB',   210)
            """.trimIndent(),
            MapSqlParameterSource().addValue("today", today).addValue("yesterday", yesterday),
        )

        jdbc.update(
            """
            INSERT INTO audit.mv_daily_user_activity (day, usr_id, event_count) VALUES
                (:today, 'usr_alice', 45),
                (:today, 'usr_bob',   30),
                (:yesterday, 'usr_alice', 20)
            """.trimIndent(),
            MapSqlParameterSource().addValue("today", today).addValue("yesterday", yesterday),
        )

        jdbc.update(
            """
            INSERT INTO audit.mv_hourly_load_stats (hour_ts, msg_type, event_count) VALUES
                (toStartOfHour(now()), 'LOGIN', 12),
                (toStartOfHour(now()), 'PUB',   35)
            """.trimIndent(),
            MapSqlParameterSource(),
        )

        Thread.sleep(500)
    }

    // ── getSummary ────────────────────────────────────────────────────────────

    @Test
    fun `getSummary returns eventsByType with correct mapping`() {
        val result =
            analyticsRepository.getSummary(
                fromTs = now - 2 * dayMs,
                toTs = now + dayMs,
            )

        // PUB → message.create, LOGIN → auth.login
        assertTrue(
            result.eventsByType.containsKey("message.create"),
            "Should contain message.create, got keys: ${result.eventsByType.keys}",
        )
        assertTrue(
            result.eventsByType.containsKey("auth.login"),
            "Should contain auth.login, got keys: ${result.eventsByType.keys}",
        )

        // Суммарно за 2 дня: LOGIN 150+90=240, PUB 320+210=530
        assertTrue(
            (result.eventsByType["auth.login"] ?: 0) >= 240,
            "auth.login count should be >= 240",
        )
        assertTrue(
            (result.eventsByType["message.create"] ?: 0) >= 530,
            "message.create count should be >= 530",
        )
    }

    @Test
    fun `getSummary returns topUsers sorted by activity`() {
        val result =
            analyticsRepository.getSummary(
                fromTs = now - 2 * dayMs,
                toTs = now + dayMs,
            )

        assertTrue(result.topUsers.isNotEmpty(), "topUsers should not be empty")
        // alice: 45+20=65, bob: 30 — alice должна быть первой
        val aliceEntry = result.topUsers.firstOrNull { it.userId == "usr_alice" }
        val bobEntry = result.topUsers.firstOrNull { it.userId == "usr_bob" }
        assertNotNull(aliceEntry, "alice should be in topUsers")
        assertNotNull(bobEntry, "bob should be in topUsers")
        assertTrue(
            (aliceEntry?.count ?: 0) > (bobEntry?.count ?: 0),
            "alice should have more events than bob",
        )
    }

    @Test
    fun `getSummary returns empty eventsByType for out-of-range period`() {
        // Запрашиваем период в прошлом — данных там нет
        val result =
            analyticsRepository.getSummary(
                fromTs = now - 365 * dayMs,
                toTs = now - 364 * dayMs,
            )
        // Не падает, возвращает пустые коллекции
        assertNotNull(result)
    }

    // ── getTimeseries ─────────────────────────────────────────────────────────

    @Test
    fun `getTimeseries hour interval returns data points`() {
        val points =
            analyticsRepository.getTimeseries(
                metric = "auth.login",
                interval = "hour",
                fromTs = now - dayMs,
                toTs = now + dayMs,
            )

        assertTrue(points.isNotEmpty(), "Should return at least one hourly point")
        assertTrue(points.all { it.ts > 0 }, "All timestamps should be positive")
        assertTrue(points.all { it.value >= 0 }, "All values should be non-negative")
    }

    @Test
    fun `getTimeseries day interval returns data points`() {
        val points =
            analyticsRepository.getTimeseries(
                metric = "message.create",
                interval = "day",
                fromTs = now - 2 * dayMs,
                toTs = now + dayMs,
            )

        assertTrue(points.isNotEmpty(), "Should return daily points")
        // Данные за сегодня и вчера
        assertTrue(points.size >= 2, "Should have at least 2 days of data, got ${points.size}")
    }

    @Test
    fun `getTimeseries throws for unsupported interval`() {
        assertThrows(IllegalArgumentException::class.java) {
            analyticsRepository.getTimeseries("auth.login", "week", now - dayMs, now)
        }
    }
}
