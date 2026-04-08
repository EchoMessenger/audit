package com.echomessenger.audit.integration

import com.echomessenger.audit.repository.IncidentRepository
import com.echomessenger.audit.service.IncidentService
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID

private fun chTs(instant: Instant): String =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
        .withZone(ZoneOffset.UTC)
        .format(instant)

class IncidentDetectionIT : IntegrationTestBase() {
    @Autowired private lateinit var incidentService: IncidentService
    @Autowired private lateinit var incidentRepository: IncidentRepository
    @Autowired private lateinit var jdbc: NamedParameterJdbcTemplate

    @BeforeEach
    fun clearIncidents() {
        // Используем уникальные данные per-тест
    }

    // ── Brute Force ───────────────────────────────────────────────────────────

    @Test
    fun `detectBruteForce creates incident for IP with 10+ failed logins in 5 minutes (password spraying)`() {
        val suspiciousIp = "192.168.${(1..254).random()}.${(1..254).random()}"
        val now = Instant.now()

        repeat(12) { i ->
            jdbc.update(
                """INSERT INTO audit.client_req_log
                   (log_id, log_timestamp, msg_type, sess_user_id, sess_auth_level, sess_remote_addr)
                   VALUES (:id, :ts, :mt, :uid, :al, :ip)""",
                MapSqlParameterSource()
                    .addValue("id", UUID.randomUUID().toString())
                    .addValue("ts", chTs(now.minusSeconds((0..180L).random())))
                    .addValue("mt", "LOGIN")
                    .addValue("uid", "victim_${UUID.randomUUID().toString().take(8)}")
                    .addValue("al", "0")
                    .addValue("ip", suspiciousIp),
            )
        }
        Thread.sleep(800)

        incidentService.runDetection()
        Thread.sleep(500)

        val incidents = incidentRepository.findAll(type = "brute_force")
        val bruteForceForIp = incidents.filter { it.details["detection_type"] == "password_spraying" && it.details["ip"] == suspiciousIp }
        assertTrue(bruteForceForIp.isNotEmpty(), "Should create brute_force incident for IP $suspiciousIp")
        assertEquals("open", bruteForceForIp.first().status)
        val count = bruteForceForIp.first().details["ip_attempt_count"]
        assertNotNull(count)
        assertTrue((count as Number).toLong() >= 10)
    }

    @Test
    fun `detectBruteForce creates incident for user with 10+ failed logins in 5 minutes (credential stuffing)`() {
        val suspiciousUserId = "target_${UUID.randomUUID().toString().take(8)}"
        val now = Instant.now()

        repeat(12) { i ->
            jdbc.update(
                """INSERT INTO audit.client_req_log
                   (log_id, log_timestamp, msg_type, sess_user_id, sess_auth_level, sess_remote_addr)
                   VALUES (:id, :ts, :mt, :uid, :al, :ip)""",
                MapSqlParameterSource()
                    .addValue("id", UUID.randomUUID().toString())
                    .addValue("ts", chTs(now.minusSeconds((0..180L).random())))
                    .addValue("mt", "LOGIN")
                    .addValue("uid", suspiciousUserId)
                    .addValue("al", "0")
                    .addValue("ip", "192.168.${(1..254).random()}.${(1..254).random()}"),
            )
        }
        Thread.sleep(800)

        incidentService.runDetection()
        Thread.sleep(500)

        val incidents = incidentRepository.findAll(type = "brute_force")
        val bruteForceForUser = incidents.filter { it.details["detection_type"] == "credential_stuffing" && it.details["user_id"] == suspiciousUserId }
        assertTrue(bruteForceForUser.isNotEmpty(), "Should create brute_force incident for user $suspiciousUserId")
        assertEquals("open", bruteForceForUser.first().status)
        val count = bruteForceForUser.first().details["user_attempt_count"]
        assertNotNull(count)
        assertTrue((count as Number).toLong() >= 10)
    }

    @Test
    fun `detectBruteForce does NOT create incident for less than threshold`() {
        val cleanIp = "10.${(1..254).random()}.${(1..254).random()}.1"
        val now = Instant.now()

        repeat(5) {
            jdbc.update(
                """INSERT INTO audit.client_req_log
                   (log_id, log_timestamp, msg_type, sess_user_id, sess_auth_level, sess_remote_addr)
                   VALUES (:id, :ts, :mt, :uid, :al, :ip)""",
                MapSqlParameterSource()
                    .addValue("id", UUID.randomUUID().toString())
                    .addValue("ts", chTs(now))
                    .addValue("mt", "LOGIN")
                    .addValue("uid", "u_${UUID.randomUUID().toString().take(6)}")
                    .addValue("al", "0")
                    .addValue("ip", cleanIp),
            )
        }
        Thread.sleep(800)

        incidentService.runDetection()
        Thread.sleep(500)

        val incidents = incidentRepository.findAll(type = "brute_force")
        val forCleanIp = incidents.filter { it.details["ip"] == cleanIp }
        assertTrue(forCleanIp.isEmpty())
    }

    // ── Mass Delete ───────────────────────────────────────────────────────────

    @Test
    fun `detectMassDelete creates incident for user with 10+ hard-deletes in 60 seconds`() {
        val userId = "mass_del_${UUID.randomUUID().toString().take(8)}"
        val now = Instant.now()

        repeat(12) { i ->
            jdbc.update(
                """INSERT INTO audit.message_log
                   (log_id, log_timestamp, action, msg_topic, msg_from_user_id,
                    msg_timestamp, msg_seq_id)
                   VALUES (:id, :ts, :act, :topic, :uid, :msgTs, :seqId)""",
                MapSqlParameterSource()
                    .addValue("id", UUID.randomUUID().toString())
                    .addValue("ts", chTs(now.minusSeconds((0..30L).random())))
                    .addValue("act", "DELETE")
                    .addValue("topic", "topic1")
                    .addValue("uid", userId)
                    .addValue("msgTs", now.minusSeconds((0..30L).random()).toEpochMilli())
                    .addValue("seqId", i + 1),
            )
        }
        Thread.sleep(800)

        incidentService.runDetection()
        Thread.sleep(500)

        val incidents = incidentRepository.findAll(type = "mass_delete", userId = userId)
        assertTrue(incidents.isNotEmpty(), "Should create mass_delete incident for $userId")
        val deleteCount = incidents.first().details["delete_count"]
        assertNotNull(deleteCount)
        assertTrue((deleteCount as Number).toLong() >= 10)
    }

    // ── Concurrent Sessions ───────────────────────────────────────────────────

    @Test
    fun `detectConcurrentSessions creates incident for user with 3+ IPs in 15 minutes`() {
        val userId = "concurrent_${UUID.randomUUID().toString().take(8)}"
        val now = Instant.now()

        listOf("192.168.1.1", "10.0.0.1", "172.16.0.1").forEach { ip ->
            repeat(2) {
                jdbc.update(
                    """INSERT INTO audit.client_req_log
                       (log_id, log_timestamp, msg_type, sess_user_id, sess_auth_level,
                        sess_remote_addr)
                       VALUES (:id, :ts, :mt, :uid, :al, :ip)""",
                    MapSqlParameterSource()
                        .addValue("id", UUID.randomUUID().toString())
                        .addValue("ts", chTs(now.minusSeconds((0..300L).random())))
                        .addValue("mt", "HI")
                        .addValue("uid", userId)
                        .addValue("al", "1")
                        .addValue("ip", ip),
                )
            }
        }
        Thread.sleep(800)

        incidentService.runDetection()
        Thread.sleep(500)

        val incidents = incidentRepository.findAll(type = "concurrent_sessions", userId = userId)
        assertTrue(incidents.isNotEmpty(), "Should create concurrent_sessions incident for $userId")
        val ipCount = incidents.first().details["unique_ips"] as? Number
        assertNotNull(ipCount)
        assertTrue((ipCount?.toLong() ?: 0) >= 3, "Should have at least 3 unique IPs, got ${ipCount?.toLong()}")
    }

    // ── Topic Enumeration ─────────────────────────────────────────────────────

    @Test
    fun `detectTopicEnumeration creates incident for user with 5+ subscription attempts in 10 minutes`() {
        val userId = "enum_${UUID.randomUUID().toString().take(8)}"
        val now = Instant.now()

        repeat(6) { i ->
            jdbc.update(
                """INSERT INTO audit.subscription_log
                   (log_id, log_timestamp, action, topic, user_id)
                   VALUES (:id, :ts, :act, :topic, :uid)""",
                MapSqlParameterSource()
                    .addValue("id", UUID.randomUUID().toString())
                    .addValue("ts", chTs(now.minusSeconds((0..300L).random())))
                    .addValue("act", "CREATE")
                    .addValue("topic", "topic_${i}")
                    .addValue("uid", userId),
            )
        }
        Thread.sleep(800)

        incidentService.runDetection()
        Thread.sleep(500)

        val incidents = incidentRepository.findAll(type = "topic_enumeration", userId = userId)
        assertTrue(incidents.isNotEmpty(), "Should create topic_enumeration incident for $userId")
        val count = incidents.first().details["subscription_attempts"]
        assertNotNull(count)
        assertTrue((count as Number).toLong() >= 5)
    }

    // ── Off-Hours Activity ─────────────────────────────────────────────────────

    @Test
    fun `detectOffHoursActivity creates incident for user with 20+ requests outside business hours`() {
        val userId = "offhours_${UUID.randomUUID().toString().take(8)}"
        // Use past 02:00 timestamp in business timezone (Europe/Moscow)
        val businessZone = ZoneId.of("Europe/Moscow")
        val now = ZonedDateTime.now(businessZone)
        val today = now.toLocalDate()
        val timeOfDay = now.toLocalTime()
        val twoAm = LocalTime.of(2, 0, 0)
        
        // Pick the most recent past 02:00
        val offHoursDate = if (timeOfDay >= twoAm) today else today.minusDays(1)
        val offHours02am = ZonedDateTime.of(offHoursDate, twoAm, businessZone)
        val offHoursTs = offHours02am.toInstant()

        repeat(22) { i ->
            jdbc.update(
                """INSERT INTO audit.client_req_log
                   (log_id, log_timestamp, msg_type, sess_user_id, sess_auth_level, sess_remote_addr)
                   VALUES (:id, :ts, :mt, :uid, :al, :ip)""",
                MapSqlParameterSource()
                    .addValue("id", UUID.randomUUID().toString())
                    .addValue("ts", chTs(offHoursTs.minusSeconds((0..300L).random())))
                    .addValue("mt", "HI")
                    .addValue("uid", userId)
                    .addValue("al", "1")
                    .addValue("ip", "10.0.0.1"),
            )
        }
        Thread.sleep(800)

        incidentService.runDetection()
        Thread.sleep(500)

        val incidents = incidentRepository.findAll(type = "off_hours_activity", userId = userId)
        assertTrue(incidents.isNotEmpty(), "Should create off_hours_activity incident for $userId")
        val count = incidents.first().details["request_count"]
        assertNotNull(count)
        assertTrue((count as Number).toLong() >= 20)
    }

    // ── Deduplication ─────────────────────────────────────────────────────────

    @Test
    fun `runDetection does not create duplicate incidents within window`() {
        val ip = "172.16.${(1..254).random()}.${(1..254).random()}"
        val now = Instant.now()

        repeat(11) {
            jdbc.update(
                """INSERT INTO audit.client_req_log
                   (log_id, log_timestamp, msg_type, sess_user_id, sess_auth_level, sess_remote_addr)
                   VALUES (:id, :ts, :mt, :uid, :al, :ip)""",
                MapSqlParameterSource()
                    .addValue("id", UUID.randomUUID().toString())
                    .addValue("ts", chTs(now))
                    .addValue("mt", "LOGIN")
                    .addValue("uid", "u_${UUID.randomUUID().toString().take(6)}")
                    .addValue("al", "0")
                    .addValue("ip", ip),
            )
        }
        Thread.sleep(800)

        incidentService.runDetection()
        Thread.sleep(300)
        incidentService.runDetection()
        Thread.sleep(500)

        val incidents = incidentRepository.findAll(type = "brute_force")
            .filter { it.details["ip"] == ip }
        assertEquals(1, incidents.size, "Should not create duplicate incidents, got ${incidents.size}")
    }
}