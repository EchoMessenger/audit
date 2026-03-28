package com.echomessenger.audit.integration

import com.echomessenger.audit.repository.AuditRepository
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import java.util.UUID
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter


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

    private fun chTs(instant: Instant): String =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
            .withZone(ZoneOffset.UTC)
            .format(instant)

    @BeforeEach
    fun seedData() {
        runId = UUID.randomUUID().toString().take(8)
        user1 = "u1_$runId"
        user2 = "u2_$runId"
        topic1 = "t1_$runId"
        topic2 = "t2_$runId"

        val now = Instant.now()

        // ── client_req_log ───────────────────────────────────────────
        // Колонки без DEFAULT (String) автоматически получают '' в ClickHouse,
        // поэтому НЕ включаем sess_language, msg_id, msg_topic и пр.

        data class ReqRow(
            val offsetSec: Long, val msgType: String, val userId: String,
            val authLevel: String, val ip: String, val ua: String,
            val sessId: String, val devId: String,
        )

        listOf(
            ReqRow(600, "LOGIN", user1, "1", "10.0.0.1", "Chrome",  "sess1", "dev1"),
            ReqRow(300, "LOGIN", user1, "0", "10.0.0.2", "Firefox", "sess1", "dev1"),
            ReqRow(180, "LOGIN", user2, "1", "10.0.0.3", "Safari",  "sess2", "dev2"),
            ReqRow(60,  "BYE",   user1, "1", "10.0.0.1", "Chrome",  "sess1", "dev1"),
        ).forEach { r ->
            jdbc.update(
                """INSERT INTO audit.client_req_log
               (log_id, log_timestamp, msg_type,
                sess_user_id, sess_auth_level,
                sess_remote_addr, sess_user_agent,
                sess_session_id, sess_device_id)
               VALUES
               (:id, :ts, :mt, :uid, :al, :ip, :ua, :sid, :did)""",
                MapSqlParameterSource()
                    .addValue("id",  UUID.randomUUID().toString())
                    .addValue("ts",  chTs(now.minusSeconds(r.offsetSec)))
                    .addValue("mt",  r.msgType)
                    .addValue("uid", r.userId)
                    .addValue("al",  r.authLevel)
                    .addValue("ip",  r.ip)
                    .addValue("ua",  r.ua)
                    .addValue("sid", r.sessId)
                    .addValue("did", r.devId),
            )
        }

        // ── message_log ──────────────────────────────────────────────
        // action — Enum8('CREATE'=0, 'UPDATE'=1, 'DELETE'=2)
        // msg_timestamp (Int64) и msg_seq_id (Int32) обязательны

        data class MsgRow(
            val offsetSec: Long, val action: String, val userId: String,
            val topic: String, val content: String, val seqId: Int,
        )

        listOf(
            MsgRow(480, "CREATE", user1, topic1, "Hello",        1),
            MsgRow(360, "CREATE", user2, topic1, "World",        2),
            MsgRow(120, "UPDATE", user1, topic1, "Hello edited", 1),
            MsgRow(60,  "DELETE", user1, topic2, "",             1),
        ).forEach { r ->
            jdbc.update(
                """INSERT INTO audit.message_log
               (log_id, log_timestamp, action,
                msg_topic, msg_from_user_id,
                msg_timestamp, msg_seq_id, msg_content)
               VALUES
               (:id, :ts, :act, :topic, :uid, :msgTs, :seqId, :content)""",
                MapSqlParameterSource()
                    .addValue("id",      UUID.randomUUID().toString())
                    .addValue("ts",      chTs(now.minusSeconds(r.offsetSec)))
                    .addValue("act",     r.action)
                    .addValue("topic",   r.topic)
                    .addValue("uid",     r.userId)
                    .addValue("msgTs",   now.minusSeconds(r.offsetSec).toEpochMilli())
                    .addValue("seqId",   r.seqId)
                    .addValue("content", r.content),
            )
        }

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

    @Test
    fun `contextual mapping resolves SUB by sub_topic`() {
        assertEquals(
            "subscription.me",
            AuditRepository.mapMsgTypeToEventType(
                msgType = "SUB",
                subTopic = "me",
                getWhat = null,
                setTopic = null,
                delWhat = null,
            ),
        )

        assertEquals(
            "topic.create",
            AuditRepository.mapMsgTypeToEventType(
                msgType = "SUB",
                subTopic = "new",
                getWhat = null,
                setTopic = null,
                delWhat = null,
            ),
        )

        assertEquals(
            "subscription.join",
            AuditRepository.mapMsgTypeToEventType(
                msgType = "SUB",
                subTopic = "grp_general",
                getWhat = null,
                setTopic = null,
                delWhat = null,
            ),
        )
    }

    @Test
    fun `contextual mapping resolves DEL by del_what`() {
        assertEquals(
            "message.delete",
            AuditRepository.mapMsgTypeToEventType(
                msgType = "DEL",
                subTopic = null,
                getWhat = null,
                setTopic = null,
                delWhat = "MSG",
            ),
        )

        assertEquals(
            "subscription.leave",
            AuditRepository.mapMsgTypeToEventType(
                msgType = "DEL",
                subTopic = null,
                getWhat = null,
                setTopic = null,
                delWhat = "SUB",
            ),
        )
    }
}
