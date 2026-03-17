package com.echomessenger.audit.integration

import com.echomessenger.audit.repository.IncidentRepository
import com.echomessenger.audit.service.IncidentService
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import java.util.UUID

/**
 * Интеграционные тесты правил детекции аномалий.
 * Вставляем реальные данные в ClickHouse и проверяем что runDetection()
 * создаёт инциденты по каждому правилу.
 *
 * Каждый тест использует уникальный IP/userId чтобы не пересекаться.
 */
class IncidentDetectionIT : IntegrationTestBase() {
    @Autowired private lateinit var incidentService: IncidentService

    @Autowired private lateinit var incidentRepository: IncidentRepository

    @Autowired private lateinit var jdbc: NamedParameterJdbcTemplate

    @BeforeEach
    fun clearIncidents() {
        // TRUNCATE не гарантирован в ClickHouse — используем уникальные данные per-тест
    }

    // ── Brute Force ───────────────────────────────────────────────────────────

    @Test
    fun `detectBruteForce creates incident for IP with 10+ failed logins in 5 minutes`() {
        val suspiciousIp = "192.168.${(1..254).random()}.${(1..254).random()}"

        // Вставляем 12 неудачных логинов с одного IP за последние 3 минуты
        repeat(12) {
            jdbc.update(
                """
                INSERT INTO audit.client_req_log
                    (log_id, req_ts, msg_type, sess_user_id, sess_auth_level, client_ip)
                VALUES
                    (generateUUIDv4(), now64(3) - INTERVAL :offset SECOND,
                     'LOGIN', :userId, 0, :ip)
                """.trimIndent(),
                MapSqlParameterSource()
                    .addValue("offset", (0..180).random())
                    .addValue("userId", "victim_${UUID.randomUUID().toString().take(8)}")
                    .addValue("ip", suspiciousIp),
            )
        }
        Thread.sleep(800)

        incidentService.runDetection()
        Thread.sleep(500)

        val incidents = incidentRepository.findAll(type = "brute_force")
        val bruteForceForIp =
            incidents.filter {
                it.details["ip"] == suspiciousIp
            }
        assertTrue(
            bruteForceForIp.isNotEmpty(),
            "Should create brute_force incident for IP $suspiciousIp",
        )
        assertEquals("open", bruteForceForIp.first().status)
        val count = bruteForceForIp.first().details["attempt_count"]
        assertNotNull(count)
        assertTrue((count as Number).toLong() >= 10)
    }

    @Test
    fun `detectBruteForce does NOT create incident for less than threshold`() {
        val cleanIp = "10.${(1..254).random()}.${(1..254).random()}.1"

        // Вставляем 5 неудачных логинов — ниже порога (10)
        repeat(5) {
            jdbc.update(
                """
                INSERT INTO audit.client_req_log
                    (log_id, req_ts, msg_type, sess_user_id, sess_auth_level, client_ip)
                VALUES (generateUUIDv4(), now64(3), 'LOGIN', :u, 0, :ip)
                """.trimIndent(),
                MapSqlParameterSource()
                    .addValue("u", "u_${UUID.randomUUID().toString().take(6)}")
                    .addValue("ip", cleanIp),
            )
        }
        Thread.sleep(800)

        incidentService.runDetection()
        Thread.sleep(500)

        val incidents = incidentRepository.findAll(type = "brute_force")
        val forCleanIp = incidents.filter { it.details["ip"] == cleanIp }
        assertTrue(
            forCleanIp.isEmpty(),
            "Should NOT create incident for IP with only 5 failed logins",
        )
    }

    // ── Mass Delete ───────────────────────────────────────────────────────────

    @Test
    fun `detectMassDelete creates incident for user with 5+ hard-deletes in 60 seconds`() {
        val userId = "mass_del_${UUID.randomUUID().toString().take(8)}"

        // Вставляем 7 HDEL за последние 30 секунд
        repeat(7) { i ->
            jdbc.update(
                """
                INSERT INTO audit.message_log
                    (seq_id, msg_ts, msg_type, usr_id, topic_id)
                VALUES (:seq, now64(3) - INTERVAL :offset SECOND, 'HDEL', :userId, 'topic1')
                """.trimIndent(),
                MapSqlParameterSource()
                    .addValue("seq", System.currentTimeMillis() * 100 + i)
                    .addValue("offset", (0..30).random())
                    .addValue("userId", userId),
            )
        }
        Thread.sleep(800)

        incidentService.runDetection()
        Thread.sleep(500)

        val incidents = incidentRepository.findAll(type = "mass_delete", userId = userId)
        assertTrue(
            incidents.isNotEmpty(),
            "Should create mass_delete incident for $userId",
        )
        val deleteCount = incidents.first().details["delete_count"]
        assertNotNull(deleteCount)
        assertTrue((deleteCount as Number).toLong() >= 5)
    }

    // ── Device Switch ─────────────────────────────────────────────────────────

    @Test
    fun `detectDeviceSwitch creates incident for session with multiple devices`() {
        val sessionId = "sess_${UUID.randomUUID().toString().take(8)}"
        val userId = "dev_switch_${UUID.randomUUID().toString().take(8)}"

        // Два разных устройства в одной сессии за последний час
        listOf("device_A", "device_B").forEach { deviceId ->
            jdbc.update(
                """
                INSERT INTO audit.client_req_log
                    (log_id, req_ts, msg_type, sess_user_id, sess_auth_level,
                     sess_session_id, sess_device_id, client_ip)
                VALUES
                    (generateUUIDv4(), now64(3) - INTERVAL :offset MINUTE,
                     'HI', :userId, 1, :sessionId, :deviceId, '10.0.0.1')
                """.trimIndent(),
                MapSqlParameterSource()
                    .addValue("offset", (1..30).random())
                    .addValue("userId", userId)
                    .addValue("sessionId", sessionId)
                    .addValue("deviceId", deviceId),
            )
        }
        Thread.sleep(800)

        incidentService.runDetection()
        Thread.sleep(500)

        val incidents = incidentRepository.findAll(type = "device_switch", userId = userId)
        assertTrue(
            incidents.isNotEmpty(),
            "Should create device_switch incident for session $sessionId",
        )
        val sessionInDetails = incidents.first().details["session_id"]
        assertEquals(sessionId, sessionInDetails)
    }

    // ── Deduplification ───────────────────────────────────────────────────────

    @Test
    fun `runDetection does not create duplicate incidents within window`() {
        val ip = "172.16.${(1..254).random()}.${(1..254).random()}"

        // Вставляем данные для brute force
        repeat(11) {
            jdbc.update(
                """
                INSERT INTO audit.client_req_log
                    (log_id, req_ts, msg_type, sess_user_id, sess_auth_level, client_ip)
                VALUES (generateUUIDv4(), now64(3), 'LOGIN', :u, 0, :ip)
                """.trimIndent(),
                MapSqlParameterSource()
                    .addValue("u", "u_${UUID.randomUUID().toString().take(6)}")
                    .addValue("ip", ip),
            )
        }
        Thread.sleep(800)

        // Запускаем детекцию дважды
        incidentService.runDetection()
        Thread.sleep(300)
        incidentService.runDetection()
        Thread.sleep(500)

        val incidents =
            incidentRepository
                .findAll(type = "brute_force")
                .filter { it.details["ip"] == ip }

        // Должен быть только один инцидент — дедупликация по existsByTypeAndUserAndWindow
        assertEquals(
            1,
            incidents.size,
            "Should not create duplicate incidents, got ${incidents.size}",
        )
    }
}
