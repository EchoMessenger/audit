package com.echomessenger.audit.service

import com.echomessenger.audit.domain.Incident
import com.echomessenger.audit.repository.IncidentRepository
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import java.util.UUID

@Service
class IncidentService(
    private val incidentRepository: IncidentRepository,
    private val jdbc: NamedParameterJdbcTemplate,
    @Value("\${audit.incidents.rules.brute-force-threshold:10}") private val bruteForceThreshold: Int,
    @Value("\${audit.incidents.rules.brute-force-window-minutes:5}") private val bruteForceWindowMinutes: Int,
    @Value("\${audit.incidents.rules.mass-delete-threshold:5}") private val massDeleteThreshold: Int,
    @Value("\${audit.incidents.rules.mass-delete-window-seconds:60}") private val massDeleteWindowSeconds: Int,
    @Value("\${audit.incidents.rules.volume-anomaly-multiplier:10.0}") private val volumeAnomalyMultiplier: Double,
) {
    private val log = LoggerFactory.getLogger(IncidentService::class.java)

    @Scheduled(fixedDelayString = "\${audit.incidents.detection-interval-seconds:300}000")
    fun runDetection() {
        log.debug("Running incident detection rules")
        try {
            detectBruteForce()
            detectDeviceSwitch()
            detectMassDelete()
            detectVolumeAnomaly()
        } catch (e: Exception) {
            log.error("Incident detection failed", e)
        }
    }

    // ── Rule: Brute Force ─────────────────────────────────────────────────────

    private fun detectBruteForce() {
        val windowMs = bruteForceWindowMinutes * 60_000L
        val params =
            MapSqlParameterSource().apply {
                addValue("windowTs", System.currentTimeMillis() - windowMs)
                addValue("threshold", bruteForceThreshold)
            }

        val suspects =
            jdbc.query(
                """
                SELECT
                    sess_remote_addr AS ip,
                    count() AS attempt_count
                FROM audit.client_req_log
                WHERE msg_type = 'LOGIN'
                  AND sess_auth_level = '0'
                  AND log_timestamp >= fromUnixTimestamp64Milli(:windowTs)
                GROUP BY sess_remote_addr
                HAVING attempt_count >= :threshold
                """.trimIndent(),
                params,
            ) { rs, _ -> rs.getString("ip") to rs.getLong("attempt_count") }

        suspects.forEach { (ip, count) ->
            if (!incidentRepository.existsByTypeAndUserAndWindow("brute_force", ip, windowMs)) {
                incidentRepository.upsert(
                    Incident(
                        incidentId = UUID.randomUUID().toString(),
                        type = "brute_force",
                        status = "open",
                        detectedAt = System.currentTimeMillis(),
                        userId = ip,
                        details =
                            mapOf(
                                "ip" to ip,
                                "attempt_count" to count,
                                "window_minutes" to bruteForceWindowMinutes,
                            ),
                        updatedAt = System.currentTimeMillis(),
                    ),
                )
                log.warn("Brute force detected from IP={} attempts={}", ip, count)
            }
        }
    }

    // ── Rule: Device Switch ───────────────────────────────────────────────────

    private fun detectDeviceSwitch() {
        val windowMs = 3_600_000L
        val params = MapSqlParameterSource("windowTs", System.currentTimeMillis() - windowMs)

        val suspects =
            jdbc.query(
                """
                SELECT sess_session_id, sess_user_id, uniqExact(sess_device_id) AS device_count
                FROM audit.client_req_log
                WHERE log_timestamp >= fromUnixTimestamp64Milli(:windowTs)
                  AND sess_session_id != ''
                  AND sess_device_id != ''
                GROUP BY sess_session_id, sess_user_id
                HAVING uniqExact(sess_device_id) > 1
                """.trimIndent(),
                params,
            ) { rs, _ ->
                Triple(
                    rs.getString("sess_session_id"),
                    rs.getString("sess_user_id"),
                    rs.getLong("device_count"),
                )
            }

        suspects.forEach { (sessionId, userId, deviceCount) ->
            if (!incidentRepository.existsByTypeAndUserAndWindow("device_switch", userId, windowMs)) {
                incidentRepository.upsert(
                    Incident(
                        incidentId = UUID.randomUUID().toString(),
                        type = "device_switch",
                        status = "open",
                        detectedAt = System.currentTimeMillis(),
                        userId = userId,
                        details =
                            mapOf(
                                "session_id" to sessionId,
                                "device_count" to deviceCount,
                            ),
                        updatedAt = System.currentTimeMillis(),
                    ),
                )
                log.warn("Device switch detected userId={} sessionId={}", userId, sessionId)
            }
        }
    }

    // ── Rule: Mass Delete ─────────────────────────────────────────────────────

    private fun detectMassDelete() {
        val windowMs = massDeleteWindowSeconds * 1000L
        val params =
            MapSqlParameterSource().apply {
                addValue("windowTs", System.currentTimeMillis() - windowMs)
                addValue("threshold", massDeleteThreshold)
            }

        val suspects =
            jdbc.query(
                """
                SELECT
                    msg_from_user_id AS user_id,
                    count() AS delete_count
                FROM audit.message_log
                WHERE toString(action) = 'DELETE'
                  AND log_timestamp >= fromUnixTimestamp64Milli(:windowTs)
                  AND msg_from_user_id IS NOT NULL
                GROUP BY msg_from_user_id
                HAVING delete_count >= :threshold
                """.trimIndent(),
                params,
            ) { rs, _ -> rs.getString("user_id") to rs.getLong("delete_count") }

        suspects.forEach { (userId, count) ->
            if (!incidentRepository.existsByTypeAndUserAndWindow("mass_delete", userId, windowMs)) {
                incidentRepository.upsert(
                    Incident(
                        incidentId = UUID.randomUUID().toString(),
                        type = "mass_delete",
                        status = "open",
                        detectedAt = System.currentTimeMillis(),
                        userId = userId,
                        details =
                            mapOf(
                                "delete_count" to count,
                                "window_seconds" to massDeleteWindowSeconds,
                            ),
                        updatedAt = System.currentTimeMillis(),
                    ),
                )
                log.warn("Mass delete detected userId={} count={}", userId, count)
            }
        }
    }

    // ── Rule: Volume Anomaly ──────────────────────────────────────────────────

    private fun detectVolumeAnomaly() {
        val currentWindowMs = 3_600_000L
        val baselineWindowMs = 30L * 24 * 3_600_000L

        val params =
            MapSqlParameterSource().apply {
                addValue("currentWindowTs", System.currentTimeMillis() - currentWindowMs)
                addValue("baselineWindowTs", System.currentTimeMillis() - baselineWindowMs)
                addValue("multiplier", volumeAnomalyMultiplier)
            }

        val suspects =
            jdbc.query(
                """
                SELECT
                    msg_from_user_id AS user_id,
                    current_count,
                    median_hourly,
                    current_count / median_hourly AS ratio
                FROM (
                    SELECT
                        msg_from_user_id,
                        countIf(hour >= toStartOfHour(fromUnixTimestamp64Milli(:currentWindowTs))) AS current_count,
                        medianExact(hourly_count) AS median_hourly
                    FROM (
                        SELECT
                            msg_from_user_id,
                            toStartOfHour(log_timestamp) AS hour,
                            count() AS hourly_count
                        FROM audit.message_log
                        WHERE log_timestamp >= fromUnixTimestamp64Milli(:baselineWindowTs)
                          AND msg_from_user_id IS NOT NULL
                        GROUP BY msg_from_user_id, hour
                    )
                    GROUP BY msg_from_user_id
                )
                WHERE median_hourly > 0
                  AND current_count > 10
                  AND current_count / median_hourly >= :multiplier
                """.trimIndent(),
                params,
            ) { rs, _ ->
                Triple(
                    rs.getString("user_id"),
                    rs.getLong("current_count"),
                    rs.getDouble("ratio"),
                )
            }

        suspects.forEach { (userId, count, ratio) ->
            if (!incidentRepository.existsByTypeAndUserAndWindow("volume_anomaly", userId, currentWindowMs)) {
                incidentRepository.upsert(
                    Incident(
                        incidentId = UUID.randomUUID().toString(),
                        type = "volume_anomaly",
                        status = "open",
                        detectedAt = System.currentTimeMillis(),
                        userId = userId,
                        details =
                            mapOf(
                                "current_hour_count" to count,
                                "median_ratio" to ratio,
                                "threshold_multiplier" to volumeAnomalyMultiplier,
                            ),
                        updatedAt = System.currentTimeMillis(),
                    ),
                )
                log.warn("Volume anomaly userId={} ratio={}", userId, ratio)
            }
        }
    }

    // ── Public API ────────────────────────────────────────────────────────────

    fun listIncidents(
        status: String?,
        type: String?,
        userId: String?,
        limit: Int,
    ) = incidentRepository.findAll(status = status, type = type, userId = userId, limit = limit)

    fun getIncident(incidentId: String) = incidentRepository.findById(incidentId)

    fun updateStatus(
        incidentId: String,
        status: String,
        comment: String?,
    ): Incident {
        val allowed = setOf("confirmed", "dismissed", "open")
        require(status in allowed) { "Invalid status: $status. Allowed: $allowed" }

        val existing =
            incidentRepository.findById(incidentId)
                ?: throw NoSuchElementException("Incident not found: $incidentId")

        val updated =
            existing.copy(
                status = status,
                details = existing.details + mapOf("admin_comment" to comment),
                updatedAt = System.currentTimeMillis(),
            )
        incidentRepository.upsert(updated)
        return updated
    }
}