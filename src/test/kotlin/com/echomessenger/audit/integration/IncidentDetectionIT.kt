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
import java.time.ZoneOffset
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
    fun `detectBruteForce creates incident for IP with 10+ failed logins in 5 minutes`() {
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
        val bruteForceForIp = incidents.filter { it.details["ip"] == suspiciousIp }
        assertTrue(bruteForceForIp.isNotEmpty(), "Should create brute_force incident for IP $suspiciousIp")
        assertEquals("open", bruteForceForIp.first().status)
        val count = bruteForceForIp.first().details["attempt_count"]
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
    fun `detectMassDelete creates incident for user with 5+ hard-deletes in 60 seconds`() {
        val userId = "mass_del_${UUID.randomUUID().toString().take(8)}"
        val now = Instant.now()

        repeat(7) { i ->
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
        assertTrue((deleteCount as Number).toLong() >= 5)
    }

    // ── Device Switch ─────────────────────────────────────────────────────────

    @Test
    fun `detectDeviceSwitch creates incident for session with multiple devices`() {
        val sessionId = "sess_${UUID.randomUUID().toString().take(8)}"
        val userId = "dev_switch_${UUID.randomUUID().toString().take(8)}"
        val now = Instant.now()

        listOf("device_A", "device_B").forEach { deviceId ->
            jdbc.update(
                """INSERT INTO audit.client_req_log
                   (log_id, log_timestamp, msg_type, sess_user_id, sess_auth_level,
                    sess_session_id, sess_device_id, sess_remote_addr)
                   VALUES (:id, :ts, :mt, :uid, :al, :sid, :did, :ip)""",
                MapSqlParameterSource()
                    .addValue("id", UUID.randomUUID().toString())
                    .addValue("ts", chTs(now.minusSeconds((60..1800L).random())))
                    .addValue("mt", "HI")
                    .addValue("uid", userId)
                    .addValue("al", "1")
                    .addValue("sid", sessionId)
                    .addValue("did", deviceId)
                    .addValue("ip", "10.0.0.1"),
            )
        }
        Thread.sleep(800)

        incidentService.runDetection()
        Thread.sleep(500)

        val incidents = incidentRepository.findAll(type = "device_switch", userId = userId)
        assertTrue(incidents.isNotEmpty(), "Should create device_switch incident for session $sessionId")
        assertEquals(sessionId, incidents.first().details["session_id"])
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