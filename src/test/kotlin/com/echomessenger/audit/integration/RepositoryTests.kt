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
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.UUID

private fun chTs(instant: Instant): String =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
        .withZone(ZoneOffset.UTC)
        .format(instant)

// ── RetentionRepositoryIT ─────────────────────────────────────────────────────

class RetentionRepositoryIT : IntegrationTestBase() {
    @Autowired private lateinit var retentionRepository: RetentionRepository

    @Test
    fun `findRetentionPolicies returns all audit tables`() {
        val policies = retentionRepository.findRetentionPolicies()
        assertNotNull(policies)
        assertTrue(policies.isNotEmpty())
        assertTrue(policies.all { it.database == "audit" })
    }

    @Test
    fun `findRetentionPolicies includes known tables`() {
        val policies = retentionRepository.findRetentionPolicies()
        val tableNames = policies.map { it.tableName }.toSet()
        val expectedTables = setOf(
            "client_req_log", "message_log", "incident_log",
            "export_job_log", "account_log", "subscription_log",
        )
        val missing = expectedTables - tableNames
        assertTrue(missing.isEmpty(), "Missing expected tables: $missing. Found: $tableNames")
    }

    @Test
    fun `findRetentionPolicies does not throw for tables without TTL`() {
        val policies = retentionRepository.findRetentionPolicies()
        val noTtl = policies.filter { it.ttlExpression.isNullOrBlank() }
        noTtl.forEach { policy ->
            assertNull(policy.retentionDays)
        }
    }

    @Test
    fun `findRetentionPolicies includes engine information`() {
        val policies = retentionRepository.findRetentionPolicies()
        assertTrue(policies.all { it.engine.isNotBlank() })
        val engines = policies.map { it.engine }.toSet()
        assertTrue(engines.any { it.contains("MergeTree") })
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
        val now = Instant.now()

        // client_req_log — 2 события (по одной строке на INSERT)
        listOf(
            Triple(600L, "LOGIN", "sess1"),
            Triple(300L, "BYE", "sess1"),
        ).forEach { (offsetSec, msgType, sessId) ->
            jdbc.update(
                """INSERT INTO audit.client_req_log
                   (log_id, log_timestamp, msg_type, sess_user_id, sess_auth_level,
                    sess_session_id, sess_device_id, sess_remote_addr)
                   VALUES (:id, :ts, :mt, :uid, :al, :sid, :did, :ip)""",
                MapSqlParameterSource()
                    .addValue("id", UUID.randomUUID().toString())
                    .addValue("ts", chTs(now.minusSeconds(offsetSec)))
                    .addValue("mt", msgType)
                    .addValue("uid", userId)
                    .addValue("al", "1")
                    .addValue("sid", sessId)
                    .addValue("did", "dev1")
                    .addValue("ip", "10.0.0.1"),
            )
        }

        // message_log — 2 события
        listOf(
            Triple(480L, "CREATE", 1),
            Triple(180L, "UPDATE", 2),
        ).forEach { (offsetSec, action, seqId) ->
            jdbc.update(
                """INSERT INTO audit.message_log
                   (log_id, log_timestamp, action, msg_topic, msg_from_user_id,
                    msg_timestamp, msg_seq_id, msg_content)
                   VALUES (:id, :ts, :act, :topic, :uid, :msgTs, :seqId, :content)""",
                MapSqlParameterSource()
                    .addValue("id", UUID.randomUUID().toString())
                    .addValue("ts", chTs(now.minusSeconds(offsetSec)))
                    .addValue("act", action)
                    .addValue("topic", "topic1")
                    .addValue("uid", userId)
                    .addValue("msgTs", now.minusSeconds(offsetSec).toEpochMilli())
                    .addValue("seqId", seqId)
                    .addValue("content", "msg $seqId"),
            )
        }

        // subscription_log — 1 событие
        jdbc.update(
            """INSERT INTO audit.subscription_log
               (log_id, log_timestamp, action, topic, user_id)
               VALUES (:id, :ts, :act, :topic, :uid)""",
            MapSqlParameterSource()
                .addValue("id", UUID.randomUUID().toString())
                .addValue("ts", chTs(now.minusSeconds(540)))
                .addValue("act", "CREATE")
                .addValue("topic", "topic1")
                .addValue("uid", userId),
        )

        Thread.sleep(1000)
    }

    @Test
    fun `getTimeline returns events from all three tables`() {
        val page = timelineRepository.getTimeline(userId = userId, limit = 100)
        assertTrue(page.data.isNotEmpty())
        assertTrue(page.data.size >= 4, "Expected at least 4 events, got ${page.data.size}")
    }

    @Test
    fun `getTimeline returns events in descending timestamp order`() {
        val page = timelineRepository.getTimeline(userId = userId, limit = 100)
        val timestamps = page.data.map { it.timestamp }
        assertEquals(timestamps.sortedDescending(), timestamps)
    }

    @Test
    fun `getTimeline filters by userId — no cross-contamination`() {
        val otherUser = "other_${UUID.randomUUID().toString().take(8)}"
        val page = timelineRepository.getTimeline(userId = otherUser, limit = 100)
        assertTrue(page.data.all { it.userId == otherUser || it.userId == null })
    }

    @Test
    fun `getTimeline cursor pagination works`() {
        val page1 = timelineRepository.getTimeline(userId = userId, limit = 2)
        assertTrue(page1.data.size <= 2)
        if (page1.hasMore) {
            assertNotNull(page1.nextCursor)
            val page2 = timelineRepository.getTimeline(
                userId = userId, cursor = page1.nextCursor, limit = 100,
            )
            val ids1 = page1.data.map { it.eventId }.toSet()
            val ids2 = page2.data.map { it.eventId }.toSet()
            assertTrue((ids1 intersect ids2).isEmpty())
        }
    }

    @Test
    fun `getTimeline fromTs toTs filter works`() {
        val now = System.currentTimeMillis()
        val page = timelineRepository.getTimeline(
            userId = userId, fromTs = now - 4 * 60 * 1000, toTs = now, limit = 100,
        )
        assertTrue(page.data.isNotEmpty())
        assertTrue(page.data.all { it.timestamp >= now - 4 * 60 * 1000 })
    }
}

// ── SessionRepositoryIT ───────────────────────────────────────────────────────

class SessionRepositoryIT : IntegrationTestBase() {
    @Autowired private lateinit var sessionRepository: SessionRepository
    @Autowired private lateinit var jdbc: NamedParameterJdbcTemplate

    @BeforeEach
    fun seedSessions() {
        val runId = UUID.randomUUID().toString().take(8)
        val now = Instant.now()
        val u1 = "sess_u1_$runId"
        val u2 = "sess_u2_$runId"
        val sA = "sessA_$runId"
        val sB = "sessB_$runId"

        data class Row(
            val offsetSec: Long, val msgType: String, val userId: String,
            val sessId: String, val devId: String, val ip: String,
        )

        listOf(
            Row(1800, "LOGIN", u1, sA, "dev1", "10.0.0.1"),
            Row(1200, "PUB",   u1, sA, "dev1", "10.0.0.1"),
            Row(600,  "BYE",   u1, sA, "dev1", "10.0.0.1"),
            Row(900,  "LOGIN", u2, sB, "dev2", "10.0.0.2"),
            Row(300,  "BYE",   u2, sB, "dev2", "10.0.0.3"),
        ).forEach { r ->
            jdbc.update(
                """INSERT INTO audit.client_req_log
                   (log_id, log_timestamp, msg_type, sess_user_id, sess_auth_level,
                    sess_session_id, sess_device_id, sess_remote_addr)
                   VALUES (:id, :ts, :mt, :uid, :al, :sid, :did, :ip)""",
                MapSqlParameterSource()
                    .addValue("id", UUID.randomUUID().toString())
                    .addValue("ts", chTs(now.minusSeconds(r.offsetSec)))
                    .addValue("mt", r.msgType)
                    .addValue("uid", r.userId)
                    .addValue("al", "1")
                    .addValue("sid", r.sessId)
                    .addValue("did", r.devId)
                    .addValue("ip", r.ip),
            )
        }
        Thread.sleep(800)
    }

    @Test
    fun `findSessions returns grouped sessions`() {
        val page = sessionRepository.findSessions(limit = 100)
        assertTrue(page.data.isNotEmpty())
        page.data.forEach { session ->
            assertTrue(session.sessionId.isNotBlank())
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
            assertTrue(kotlin.math.abs(session.durationSeconds - expectedDuration) <= 1)
        }
    }

    @Test
    fun `findSessions filters by userId`() {
        val page = sessionRepository.findSessions(
            userId = "nonexistent_user_xyz_${UUID.randomUUID()}", limit = 100,
        )
        assertTrue(page.data.isEmpty())
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
            assertTrue((ids1 intersect ids2).isEmpty())
        }
    }
}

// ── MessageRepositoryIT ───────────────────────────────────────────────────────

class MessageRepositoryIT : IntegrationTestBase() {
    @Autowired private lateinit var messageRepository: MessageRepository
    @Autowired private lateinit var jdbc: NamedParameterJdbcTemplate

    private lateinit var userId: String
    private lateinit var topicId: String
    private var seedTime: Long = 0

    @BeforeEach
    fun seedMessages() {
        userId = "msg_u_${UUID.randomUUID().toString().take(8)}"
        topicId = "msg_t_${UUID.randomUUID().toString().take(8)}"
        val instant = Instant.now()
        seedTime = instant.toEpochMilli()

        data class MsgRow(
            val offsetSec: Long, val action: String, val seqId: Int, val content: String,
        )

        listOf(
            MsgRow(10800, "CREATE", 1, "First message"),
            MsgRow(7200,  "UPDATE", 1, "Edited message"),
            MsgRow(3600,  "CREATE", 2, "Second message"),
            MsgRow(1800,  "DELETE", 3, ""),
        ).forEach { r ->
            jdbc.update(
                """INSERT INTO audit.message_log
                   (log_id, log_timestamp, action, msg_topic, msg_from_user_id,
                    msg_timestamp, msg_seq_id, msg_content)
                   VALUES (:id, :ts, :act, :topic, :uid, :msgTs, :seqId, :content)""",
                MapSqlParameterSource()
                    .addValue("id", UUID.randomUUID().toString())
                    .addValue("ts", chTs(instant.minusSeconds(r.offsetSec)))
                    .addValue("act", r.action)
                    .addValue("topic", topicId)
                    .addValue("uid", userId)
                    .addValue("msgTs", instant.minusSeconds(r.offsetSec).toEpochMilli())
                    .addValue("seqId", r.seqId)
                    .addValue("content", r.content),
            )
        }
        Thread.sleep(800)
    }

    @Test
    fun `findMessages returns messages in time range`() {
        val req = MessageReportRequest(
            users = listOf(userId),
            fromTs = seedTime - 4 * 3600_000,
            toTs = seedTime + 60_000,
        )
        val messages = messageRepository.findMessages(req)
        assertTrue(messages.isNotEmpty())
        assertTrue(messages.all { it.userId == userId })
    }

    @Test
    fun `findMessages excludes hard-deleted by default`() {
        val req = MessageReportRequest(
            users = listOf(userId),
            fromTs = seedTime - 4 * 3600_000,
            toTs = seedTime + 60_000,
            includeDeleted = false,
        )
        val messages = messageRepository.findMessages(req)
        assertTrue(messages.none { it.isDeleted })
        assertEquals(3, messages.size, "Should have 3 non-deleted messages")
    }

    @Test
    fun `findMessages includes hard-deleted when requested`() {
        val req = MessageReportRequest(
            users = listOf(userId),
            fromTs = seedTime - 4 * 3600_000,
            toTs = seedTime + 60_000,
            includeDeleted = true,
        )
        val messages = messageRepository.findMessages(req)
        assertTrue(messages.any { it.isDeleted })
        assertEquals(4, messages.size, "Should have 4 messages with includeDeleted=true")
    }

    @Test
    fun `countMessages returns correct count`() {
        val req = MessageReportRequest(
            users = listOf(userId),
            fromTs = seedTime - 4 * 3600_000,
            toTs = seedTime + 60_000,
            includeDeleted = false,
        )
        val count = messageRepository.countMessages(req)
        val messages = messageRepository.findMessages(req)
        assertEquals(messages.size.toLong(), count)
    }

    @Test
    fun `streamMessages delivers all messages in batches`() {
        val req = MessageReportRequest(
            users = listOf(userId),
            fromTs = seedTime - 4 * 3600_000,
            toTs = seedTime + 60_000,
            includeDeleted = true,
        )
        val collected = mutableListOf<Long>()
        messageRepository.streamMessages(req, batchSize = 2) { batch ->
            collected.addAll(batch.map { it.messageId })
        }
        assertEquals(3, collected.size, "Expected 3 messages for $userId")
        assertEquals(collected.size, collected.toSet().size, "No duplicates")
    }

    @Test
    fun `findMessages filters by topic`() {
        val req = MessageReportRequest(
            topics = listOf(topicId),
            fromTs = seedTime - 4 * 3600_000,
            toTs = seedTime + 60_000,
        )
        val messages = messageRepository.findMessages(req)
        assertTrue(messages.isNotEmpty())
        assertTrue(messages.all { it.topicId == topicId })
    }

    @Test
    fun `findMessages returns empty for out-of-range period`() {
        val req = MessageReportRequest(
            users = listOf(userId),
            fromTs = seedTime - 365 * 86400_000L,
            toTs = seedTime - 364 * 86400_000L,
        )
        val messages = messageRepository.findMessages(req)
        assertTrue(messages.isEmpty())
    }
}