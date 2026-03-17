package com.echomessenger.audit.integration

import com.echomessenger.audit.repository.AuditRepository
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import java.util.UUID

class AuditRepositoryIT : IntegrationTestBase() {
    @Autowired private lateinit var auditRepository: AuditRepository

    @Autowired private lateinit var jdbc: NamedParameterJdbcTemplate

    // Уникальный префикс для каждого теста — решает проблему TRUNCATE в ClickHouse.
    // ClickHouse TRUNCATE асинхронный, данные из предыдущих тестов могут оставаться.
    // Вместо попытки изолировать через очистку — используем уникальные userId/topicId
    // и фильтруем только по ним. Это стандартная практика для ClickHouse интеграционных тестов.
    private lateinit var runId: String
    private lateinit var user1: String
    private lateinit var user2: String
    private lateinit var topic1: String
    private lateinit var topic2: String

    @BeforeEach
    fun seedData() {
        runId = UUID.randomUUID().toString().take(8)
        user1 = "u1_$runId"
        user2 = "u2_$runId"
        topic1 = "t1_$runId"
        topic2 = "t2_$runId"

        jdbc.update(
            """
            INSERT INTO audit.client_req_log
                (log_id, req_ts, msg_type, sess_user_id, sess_auth_level, client_ip, user_agent, sess_session_id, sess_device_id)
            VALUES
                (generateUUIDv4(), now64(3) - INTERVAL 10 MINUTE, 'LOGIN', :u1, 1, '10.0.0.1', 'Chrome',  'sess1', 'dev1'),
                (generateUUIDv4(), now64(3) - INTERVAL 5 MINUTE,  'LOGIN', :u1, 0, '10.0.0.2', 'Firefox', 'sess1', 'dev1'),
                (generateUUIDv4(), now64(3) - INTERVAL 3 MINUTE,  'LOGIN', :u2, 1, '10.0.0.3', 'Safari',  'sess2', 'dev2'),
                (generateUUIDv4(), now64(3) - INTERVAL 1 MINUTE,  'BYE',   :u1, 1, '10.0.0.1', 'Chrome',  'sess1', 'dev1')
            """.trimIndent(),
            MapSqlParameterSource().addValue("u1", user1).addValue("u2", user2),
        )

        jdbc.update(
            """
            INSERT INTO audit.message_log
                (seq_id, msg_ts, msg_type, usr_id, topic_id, content)
            VALUES
                (:s1, now64(3) - INTERVAL 8 MINUTE, 'PUB',  :u1, :t1, 'Hello'),
                (:s2, now64(3) - INTERVAL 6 MINUTE, 'PUB',  :u2, :t1, 'World'),
                (:s3, now64(3) - INTERVAL 2 MINUTE, 'EDIT', :u1, :t1, 'Hello edited'),
                (:s4, now64(3) - INTERVAL 1 MINUTE, 'DEL',  :u1, :t2, '')
            """.trimIndent(),
            MapSqlParameterSource()
                .addValue("u1", user1)
                .addValue("u2", user2)
                .addValue("t1", topic1)
                .addValue("t2", topic2)
                // seq_id уникальный per-run чтобы не конфликтовать с другими тестами
                .addValue("s1", runId.hashCode().toLong().and(0xFFFFFFFFL) * 10 + 1)
                .addValue("s2", runId.hashCode().toLong().and(0xFFFFFFFFL) * 10 + 2)
                .addValue("s3", runId.hashCode().toLong().and(0xFFFFFFFFL) * 10 + 3)
                .addValue("s4", runId.hashCode().toLong().and(0xFFFFFFFFL) * 10 + 4),
        )

        // ClickHouse буферизует записи — ждём flush
        Thread.sleep(1000)
    }

    // ── findAuthEvents ────────────────────────────────────────────────────────

    @Test
    fun `findAuthEvents returns only auth event types`() {
        // Фильтруем только наши тестовые данные через userId
        val p1 = auditRepository.findAuthEvents(userId = user1, limit = 100)
        val p2 = auditRepository.findAuthEvents(userId = user2, limit = 100)
        val page = p1.copy(data = p1.data + p2.data)

        val eventTypes = page.data.map { it.eventType }.toSet()
        assertTrue(
            eventTypes.all { it.startsWith("auth.") },
            "All auth events should start with 'auth.', got: $eventTypes",
        )
        // user1: LOGIN, LOGIN, BYE = 3; user2: LOGIN = 1 → итого 4
        assertEquals(4, page.data.size, "Expected 4 auth events, got ${page.data.size}")
    }

    @Test
    fun `findAuthEvents filters by userId`() {
        val page = auditRepository.findAuthEvents(userId = user1, limit = 100)

        assertTrue(page.data.all { it.userId == user1 }, "All events should belong to $user1")
        assertEquals(3, page.data.size, "Expected 3 events for user1, got ${page.data.size}")
    }

    @Test
    fun `findAuthEvents returns ip and userAgent fields`() {
        val page = auditRepository.findAuthEvents(userId = user1, limit = 10)

        val loginEvent = page.data.firstOrNull { it.eventType == "auth.login" }
        assertNotNull(loginEvent, "Should find at least one login event for $user1")
        assertNotNull(loginEvent!!.ip, "IP should not be null")
        assertNotNull(loginEvent.userAgent, "User-Agent should not be null")
    }

    // ── findEvents cursor pagination ──────────────────────────────────────────

    @Test
    fun `findEvents returns correct page size`() {
        // Запрашиваем только наши данные через userId=user1 (3 client_req + 3 message = 6 событий)
        // Берём страницу в 3 — должно быть ровно 3 и hasMore=true
        val page = auditRepository.findEvents(userId = user1, limit = 3)

        assertEquals(3, page.data.size, "Page size should be 3, got ${page.data.size}")
        assertTrue(page.hasMore, "Should have more results for user1")
        assertNotNull(page.nextCursor, "Next cursor should not be null")
    }

    @Test
    fun `findEvents cursor pagination traverses all results`() {
        // user1: 3 client_req (LOGIN, LOGIN, BYE) + 3 message (PUB, EDIT, DEL) = 6
        val allEvents = mutableListOf<String>()
        var cursor: String? = null
        var iterations = 0

        do {
            val page = auditRepository.findEvents(userId = user1, cursor = cursor, limit = 2)
            allEvents.addAll(page.data.map { it.eventId })
            cursor = page.nextCursor
            iterations++
        } while (cursor != null && iterations < 10)

        assertEquals(6, allEvents.size, "Expected 6 events for user1, got ${allEvents.size}")
        assertEquals(allEvents.size, allEvents.toSet().size, "Found duplicate event IDs: $allEvents")
    }

    @Test
    fun `findEvents filters by userId`() {
        val page = auditRepository.findEvents(userId = user1, limit = 100)

        assertTrue(page.data.isNotEmpty(), "Should find events for $user1")
        assertTrue(page.data.all { it.userId == user1 }, "All events should belong to $user1")
    }

    @Test
    fun `findEvents filters by topicId`() {
        // topic1 имеет PUB user1, PUB user2, EDIT user1 = 3 события
        val page = auditRepository.findEvents(topicId = topic1, limit = 100)

        assertTrue(page.data.all { it.topicId == topic1 }, "All events should have topicId=$topic1")
        assertEquals(3, page.data.size, "Expected 3 events for topic1, got ${page.data.size}")
    }

    // ── findEventById ─────────────────────────────────────────────────────────

    @Test
    fun `findEventById returns null for nonexistent id`() {
        val result = auditRepository.findEventById("00000000-0000-0000-0000-000000000000")
        assertNull(result, "Should return null for nonexistent UUID")
    }

    // ── event type mapping (pure unit, no DB) ─────────────────────────────────

    @Test
    fun `mapMsgTypeToEventType covers all known types`() {
        val mappings =
            mapOf(
                "LOGIN" to "auth.login",
                "HI" to "auth.session_start",
                "BYE" to "auth.logout",
                "REG" to "auth.register",
                "PUB" to "message.create",
                "EDIT" to "message.edit",
                "DEL" to "message.delete",
                "HDEL" to "message.hard_delete",
            )
        mappings.forEach { (msgType, expected) ->
            assertEquals(
                expected,
                AuditRepository.mapMsgTypeToEventType(msgType),
                "msg_type=$msgType should map to event_type=$expected",
            )
        }
    }

    @Test
    fun `mapEventTypeToMsgTypes is inverse of mapMsgTypeToEventType`() {
        val eventType = "message.create"
        val msgTypes = AuditRepository.mapEventTypeToMsgTypes(eventType)
        assertNotNull(msgTypes, "Should return msgTypes for event_type=$eventType")
        assertTrue(msgTypes!!.isNotEmpty(), "msgTypes should not be empty")
        assertEquals(eventType, AuditRepository.mapMsgTypeToEventType(msgTypes.first()))
    }
}
