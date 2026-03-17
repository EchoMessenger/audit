package com.echomessenger.audit.integration

import com.echomessenger.audit.domain.MessageReportRequest
import com.echomessenger.audit.repository.MessageRepository
import com.echomessenger.audit.repository.RetentionRepository
import com.echomessenger.audit.repository.SessionRepository
import com.echomessenger.audit.repository.UserTimelineRepository
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import java.util.UUID

// ── RetentionRepositoryIT ─────────────────────────────────────────────────────

class RetentionRepositoryIT : IntegrationTestBase() {
    @Autowired private lateinit var retentionRepository: RetentionRepository

    @Test
    fun `findRetentionPolicies returns all audit tables`() {
        val policies = retentionRepository.findRetentionPolicies()

        assertNotNull(policies, "Should return non-null list")
        assertTrue(policies.isNotEmpty(), "Should find at least one table in audit database")

        // Все записи принадлежат схеме audit
        assertTrue(
            policies.all { it.database == "audit" },
            "All policies should be from audit database",
        )
    }

    @Test
    fun `findRetentionPolicies includes known tables`() {
        val policies = retentionRepository.findRetentionPolicies()
        val tableNames = policies.map { it.tableName }.toSet()

        val expectedTables =
            setOf(
                "client_req_log",
                "message_log",
                "incident_log",
                "export_job_log",
                "account_log",
                "subscription_log",
            )
        val missing = expectedTables - tableNames
        assertTrue(
            missing.isEmpty(),
            "Missing expected tables: $missing. Found: $tableNames",
        )
    }

    @Test
    fun `findRetentionPolicies does not throw for tables without TTL`() {
        // Таблицы без TTL должны возвращать retentionDays=null, не падать
        val policies = retentionRepository.findRetentionPolicies()
        val noTtl = policies.filter { it.ttlExpression.isNullOrBlank() }

        // Просто проверяем что они корректно представлены
        noTtl.forEach { policy ->
            assertNull(
                policy.retentionDays,
                "Tables without TTL should have retentionDays=null",
            )
        }
    }

    @Test
    fun `findRetentionPolicies includes engine information`() {
        val policies = retentionRepository.findRetentionPolicies()

        assertTrue(
            policies.all { it.engine.isNotBlank() },
            "All policies should have engine info",
        )
        // В тестовой схеме используем MergeTree и ReplacingMergeTree
        val engines = policies.map { it.engine }.toSet()
        assertTrue(
            engines.any { it.contains("MergeTree") },
            "Should have at least one MergeTree table, got: $engines",
        )
    }
}

// ── UserTimelineRepositoryIT ──────────────────────────────────────────────────

class UserTimelineRepositoryIT : IntegrationTestBase() {
    @Autowired private lateinit var timelineRepository: UserTimelineRepository

    @Autowired private lateinit var jdbc: NamedParameterJdbcTemplate

    private lateinit var userId: String

    @BeforeEach
    fun seedTimeline() {
        userId = "timeline_${UUID.randomUUID().toString().take(8)}"

        // client_req_log — 2 события
        jdbc.update(
            """
            INSERT INTO audit.client_req_log
                (log_id, req_ts, msg_type, sess_user_id, sess_auth_level,
                 sess_session_id, sess_device_id, client_ip)
            VALUES
                (generateUUIDv4(), now64(3) - INTERVAL 10 MINUTE, 'LOGIN', :u, 1, 'sess1', 'dev1', '10.0.0.1'),
                (generateUUIDv4(), now64(3) - INTERVAL 5 MINUTE,  'BYE',   :u, 1, 'sess1', 'dev1', '10.0.0.1')
            """.trimIndent(),
            MapSqlParameterSource("u", userId),
        )

        // message_log — 2 события
        val base = System.currentTimeMillis() * 1000 + userId.hashCode().and(0xFFFF)
        jdbc.update(
            """
            INSERT INTO audit.message_log
                (seq_id, msg_ts, msg_type, usr_id, topic_id, content)
            VALUES
                (:s1, now64(3) - INTERVAL 8 MINUTE, 'PUB',  :u, 'topic1', 'Hello'),
                (:s2, now64(3) - INTERVAL 3 MINUTE, 'EDIT', :u, 'topic1', 'Hello edited')
            """.trimIndent(),
            MapSqlParameterSource("u", userId)
                .addValue("s1", base + 1)
                .addValue("s2", base + 2),
        )

        // subscription_log — 1 событие
        jdbc.update(
            """
            INSERT INTO audit.subscription_log
                (event_ts, user_id, topic_id, action, new_role, old_role)
            VALUES
                (now64(3) - INTERVAL 9 MINUTE, :u, 'topic1', 'JOIN', 'member', '')
            """.trimIndent(),
            MapSqlParameterSource("u", userId),
        )

        Thread.sleep(1000)
    }

    @Test
    fun `getTimeline returns events from all three tables`() {
        val page = timelineRepository.getTimeline(userId = userId, limit = 100)

        assertTrue(page.data.isNotEmpty(), "Timeline should not be empty")
        // Ожидаем минимум 4 события (2 client_req + 2 message + 1 subscription = 5)
        assertTrue(
            page.data.size >= 4,
            "Expected at least 4 events, got ${page.data.size}",
        )
    }

    @Test
    fun `getTimeline returns events in descending timestamp order`() {
        val page = timelineRepository.getTimeline(userId = userId, limit = 100)

        val timestamps = page.data.map { it.timestamp }
        val sorted = timestamps.sortedDescending()
        assertEquals(sorted, timestamps, "Events should be in descending timestamp order")
    }

    @Test
    fun `getTimeline filters by userId — no cross-contamination`() {
        val otherUser = "other_${UUID.randomUUID().toString().take(8)}"
        val page = timelineRepository.getTimeline(userId = otherUser, limit = 100)

        assertTrue(
            page.data.all { it.userId == otherUser || it.userId == null },
            "Should only return events for requested user",
        )
    }

    @Test
    fun `getTimeline cursor pagination works`() {
        val page1 = timelineRepository.getTimeline(userId = userId, limit = 2)

        assertTrue(page1.data.size <= 2)
        if (page1.hasMore) {
            assertNotNull(page1.nextCursor)
            val page2 =
                timelineRepository.getTimeline(
                    userId = userId,
                    cursor = page1.nextCursor,
                    limit = 100,
                )
            // Нет пересечений между страницами
            val ids1 = page1.data.map { it.eventId }.toSet()
            val ids2 = page2.data.map { it.eventId }.toSet()
            assertTrue(
                (ids1 intersect ids2).isEmpty(),
                "Pages should not contain duplicate events",
            )
        }
    }

    @Test
    fun `getTimeline fromTs toTs filter works`() {
        val now = System.currentTimeMillis()
        // Запрашиваем только последние 4 минуты — должны попасть BYE и EDIT
        val page =
            timelineRepository.getTimeline(
                userId = userId,
                fromTs = now - 4 * 60 * 1000,
                toTs = now,
                limit = 100,
            )

        assertTrue(page.data.isNotEmpty())
        // Все события в запрошенном диапазоне
        assertTrue(
            page.data.all { it.timestamp >= now - 4 * 60 * 1000 },
            "All events should be within requested time range",
        )
    }
}

// ── SessionRepositoryIT ───────────────────────────────────────────────────────

class SessionRepositoryIT : IntegrationTestBase() {
    @Autowired private lateinit var sessionRepository: SessionRepository

    @Autowired private lateinit var jdbc: NamedParameterJdbcTemplate

    @BeforeEach
    fun seedSessions() {
        val runId = UUID.randomUUID().toString().take(8)

        jdbc.update(
            """
            INSERT INTO audit.client_req_log
                (log_id, req_ts, msg_type, sess_user_id, sess_auth_level,
                 sess_session_id, sess_device_id, client_ip)
            VALUES
                (generateUUIDv4(), now64(3) - INTERVAL 30 MINUTE, 'LOGIN', :u1, 1, :sA, 'dev1', '10.0.0.1'),
                (generateUUIDv4(), now64(3) - INTERVAL 20 MINUTE, 'PUB',   :u1, 1, :sA, 'dev1', '10.0.0.1'),
                (generateUUIDv4(), now64(3) - INTERVAL 10 MINUTE, 'BYE',   :u1, 1, :sA, 'dev1', '10.0.0.1'),
                (generateUUIDv4(), now64(3) - INTERVAL 15 MINUTE, 'LOGIN', :u2, 1, :sB, 'dev2', '10.0.0.2'),
                (generateUUIDv4(), now64(3) - INTERVAL 5 MINUTE,  'BYE',   :u2, 1, :sB, 'dev2', '10.0.0.3')
            """.trimIndent(),
            MapSqlParameterSource()
                .addValue("u1", "sess_u1_$runId")
                .addValue("u2", "sess_u2_$runId")
                .addValue("sA", "sessA_$runId")
                .addValue("sB", "sessB_$runId"),
        )
        Thread.sleep(800)
    }

    @Test
    fun `findSessions returns grouped sessions`() {
        val page = sessionRepository.findSessions(limit = 100)

        assertTrue(page.data.isNotEmpty(), "Should return sessions")
        // У каждой сессии есть обязательные поля
        page.data.forEach { session ->
            assertTrue(session.sessionId.isNotBlank(), "sessionId should not be blank")
            assertTrue(session.firstEventAt > 0)
            assertTrue(session.lastEventAt >= session.firstEventAt)
            assertTrue(session.eventCount > 0)
        }
    }

    @Test
    fun `findSessions calculates duration correctly`() {
        val page = sessionRepository.findSessions(limit = 100)

        page.data.forEach { session ->
            val expectedDuration = (session.lastEventAt - session.firstEventAt) / 1000
            // Допускаем погрешность в 1 секунду из-за округления
            assertTrue(
                kotlin.math.abs(session.durationSeconds - expectedDuration) <= 1,
                "Duration should match difference between first and last event",
            )
        }
    }

    @Test
    fun `findSessions filters by userId`() {
        // Используем уникальный userId из текущего теста — сложно без runId
        // Проверяем что фильтр userId вообще не падает
        val page =
            sessionRepository.findSessions(
                userId = "nonexistent_user_xyz_${UUID.randomUUID()}",
                limit = 100,
            )
        assertTrue(page.data.isEmpty(), "Should return empty for nonexistent user")
        assertFalse(page.hasMore)
    }

    @Test
    fun `findSessions cursor pagination works`() {
        val page1 = sessionRepository.findSessions(limit = 1)

        if (page1.hasMore) {
            assertNotNull(page1.nextCursor)
            val page2 = sessionRepository.findSessions(cursor = page1.nextCursor, limit = 100)
            val ids1 = page1.data.map { it.sessionId }.toSet()
            val ids2 = page2.data.map { it.sessionId }.toSet()
            assertTrue((ids1 intersect ids2).isEmpty(), "Pages should not overlap")
        }
    }
}

// ── MessageRepositoryIT ───────────────────────────────────────────────────────

class MessageRepositoryIT : IntegrationTestBase() {
    @Autowired private lateinit var messageRepository: MessageRepository

    @Autowired private lateinit var jdbc: NamedParameterJdbcTemplate

    private lateinit var userId: String
    private lateinit var topicId: String
    private val now = System.currentTimeMillis()

    @BeforeEach
    fun seedMessages() {
        userId = "msg_u_${UUID.randomUUID().toString().take(8)}"
        topicId = "msg_t_${UUID.randomUUID().toString().take(8)}"
        val base = now * 100 + userId.hashCode().and(0xFFFF).toLong()

        jdbc.update(
            """
            INSERT INTO audit.message_log
                (seq_id, msg_ts, msg_type, usr_id, topic_id, content)
            VALUES
                (:s1, now64(3) - INTERVAL 3 HOUR, 'PUB',  :u, :t, 'First message'),
                (:s2, now64(3) - INTERVAL 2 HOUR, 'EDIT', :u, :t, 'Edited message'),
                (:s3, now64(3) - INTERVAL 1 HOUR, 'PUB',  :u, :t, 'Second message'),
                (:s4, now64(3) - INTERVAL 30 MINUTE, 'HDEL', :u, :t, '')
            """.trimIndent(),
            MapSqlParameterSource("u", userId)
                .addValue("t", topicId)
                .addValue("s1", base + 1)
                .addValue("s2", base + 2)
                .addValue("s3", base + 3)
                .addValue("s4", base + 4),
        )
        Thread.sleep(800)
    }

    @Test
    fun `findMessages returns messages in time range`() {
        val req =
            MessageReportRequest(
                users = listOf(userId),
                fromTs = now - 4 * 3600_000,
                toTs = now,
            )
        val messages = messageRepository.findMessages(req)

        assertTrue(messages.isNotEmpty(), "Should find messages for $userId")
        assertTrue(messages.all { it.userId == userId })
    }

    @Test
    fun `findMessages excludes hard-deleted by default`() {
        val req =
            MessageReportRequest(
                users = listOf(userId),
                fromTs = now - 4 * 3600_000,
                toTs = now,
                includeDeleted = false,
            )
        val messages = messageRepository.findMessages(req)

        assertTrue(
            messages.none { it.isDeleted },
            "Should not include hard-deleted messages by default",
        )
    }

    @Test
    fun `findMessages includes hard-deleted when requested`() {
        val req =
            MessageReportRequest(
                users = listOf(userId),
                fromTs = now - 4 * 3600_000,
                toTs = now,
                includeDeleted = true,
            )
        val messages = messageRepository.findMessages(req)

        assertTrue(
            messages.any { it.isDeleted },
            "Should include hard-deleted messages when includeDeleted=true",
        )
    }

    @Test
    fun `countMessages returns correct count`() {
        val req =
            MessageReportRequest(
                users = listOf(userId),
                fromTs = now - 4 * 3600_000,
                toTs = now,
                includeDeleted = false,
            )
        val count = messageRepository.countMessages(req)
        val messages = messageRepository.findMessages(req)

        assertEquals(
            messages.size.toLong(),
            count,
            "countMessages should match findMessages size",
        )
    }

    @Test
    fun `streamMessages delivers all messages in batches`() {
        val req =
            MessageReportRequest(
                users = listOf(userId),
                fromTs = now - 4 * 3600_000,
                toTs = now,
                includeDeleted = true,
            )
        val collected = mutableListOf<Long>()
        messageRepository.streamMessages(req, batchSize = 2) { batch ->
            collected.addAll(batch.map { it.messageId })
        }

        assertTrue(collected.isNotEmpty(), "streamMessages should deliver at least one batch")
        assertEquals(collected.size, collected.toSet().size, "No duplicate messages in stream")
    }

    @Test
    fun `findMessages filters by topic`() {
        val req =
            MessageReportRequest(
                topics = listOf(topicId),
                fromTs = now - 4 * 3600_000,
                toTs = now,
            )
        val messages = messageRepository.findMessages(req)

        assertTrue(messages.isNotEmpty())
        assertTrue(messages.all { it.topicId == topicId })
    }

    @Test
    fun `findMessages returns empty for out-of-range period`() {
        val req =
            MessageReportRequest(
                users = listOf(userId),
                fromTs = now - 365 * 86400_000L,
                toTs = now - 364 * 86400_000L,
            )
        val messages = messageRepository.findMessages(req)
        assertTrue(messages.isEmpty(), "Should return empty for period with no data")
    }
}
