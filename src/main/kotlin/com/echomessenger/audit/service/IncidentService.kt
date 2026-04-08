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
    // R1: Brute Force
    @Value("\${audit.incidents.rules.brute-force-threshold:10}") private val bruteForceThreshold: Int,
    @Value("\${audit.incidents.rules.brute-force-window-minutes:5}") private val bruteForceWindowMinutes: Int,
    // R2: Concurrent Sessions
    @Value("\${audit.incidents.rules.concurrent-sessions-threshold:3}") private val concurrentSessionsThreshold: Int,
    @Value("\${audit.incidents.rules.concurrent-sessions-window-minutes:15}") private val concurrentSessionsWindowMinutes: Int,
    // R3: Mass Delete
    @Value("\${audit.incidents.rules.mass-delete-threshold:10}") private val massDeleteThreshold: Int,
    @Value("\${audit.incidents.rules.mass-delete-window-seconds:60}") private val massDeleteWindowSeconds: Int,
    // R4: Volume Anomaly
    @Value("\${audit.incidents.rules.volume-anomaly-multiplier:10.0}") private val volumeAnomalyMultiplier: Double,
    @Value("\${audit.incidents.rules.volume-anomaly-min-threshold:50}") private val volumeAnomalyMinThreshold: Int,
    // R5: Topic Enumeration
    @Value("\${audit.incidents.rules.topic-enumeration-threshold:5}") private val topicEnumerationThreshold: Int,
    @Value("\${audit.incidents.rules.topic-enumeration-window-minutes:10}") private val topicEnumerationWindowMinutes: Int,
    // R6: Inactive Account Activation
    @Value("\${audit.incidents.rules.inactive-activation-inactivity-days:30}") private val inactiveActivationInactivityDays: Int,
    @Value("\${audit.incidents.rules.inactive-activation-multiplier:5.0}") private val inactiveActivationMultiplier: Double,
    // R7: Off-Hours Activity
    @Value("\${audit.incidents.rules.off-hours-start:09:00}") private val offHoursStart: String,
    @Value("\${audit.incidents.rules.off-hours-end:19:00}") private val offHoursEnd: String,
    @Value("\${audit.incidents.rules.off-hours-time-zone:Europe/Moscow}") private val offHoursTimeZone: String,
    @Value("\${audit.incidents.rules.off-hours-threshold:20}") private val offHoursThreshold: Int,
) {
    private val log = LoggerFactory.getLogger(IncidentService::class.java)

    @Scheduled(fixedDelayString = "\${audit.incidents.detection-interval-seconds:300}000")
    fun runDetection() {
        log.debug("Running incident detection rules")
        
        try {
            detectBruteForce()
        } catch (e: Exception) {
            log.error("Detector failed: detectBruteForce", e)
        }
        
        try {
            detectConcurrentSessions()
        } catch (e: Exception) {
            log.error("Detector failed: detectConcurrentSessions", e)
        }
        
        try {
            detectMassDelete()
        } catch (e: Exception) {
            log.error("Detector failed: detectMassDelete", e)
        }
        
        try {
            detectVolumeAnomaly()
        } catch (e: Exception) {
            log.error("Detector failed: detectVolumeAnomaly", e)
        }
        
        try {
            detectTopicEnumeration()
        } catch (e: Exception) {
            log.error("Detector failed: detectTopicEnumeration", e)
        }
        
        try {
            detectInactiveAccountActivation()
        } catch (e: Exception) {
            log.error("Detector failed: detectInactiveAccountActivation", e)
        }
        
        try {
            detectOffHoursActivity()
        } catch (e: Exception) {
            log.error("Detector failed: detectOffHoursActivity", e)
        }
        
        try {
            detectPrivilegeEscalation()
        } catch (e: Exception) {
            log.error("Detector failed: detectPrivilegeEscalation", e)
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

        // Detector 1: By IP (password spraying)
        val ipSuspects =
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

        // Detector 2: By user_id (credential stuffing)
        val userSuspects =
            jdbc.query(
                """
                SELECT
                    sess_user_id AS user_id,
                    count() AS attempt_count
                FROM audit.client_req_log
                WHERE msg_type = 'LOGIN'
                  AND sess_auth_level = '0'
                  AND log_timestamp >= fromUnixTimestamp64Milli(:windowTs)
                  AND sess_user_id != ''
                GROUP BY sess_user_id
                HAVING attempt_count >= :threshold
                """.trimIndent(),
                params,
            ) { rs, _ -> rs.getString("user_id") to rs.getLong("attempt_count") }

        ipSuspects.forEach { (ip, ipCount) ->
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
                                "detection_type" to "password_spraying",
                                "ip" to ip,
                                "ip_attempt_count" to ipCount,
                                "window_minutes" to bruteForceWindowMinutes,
                            ),
                        updatedAt = System.currentTimeMillis(),
                    ),
                )
                log.warn("Brute force (password spraying) detected from IP={} attempts={}", ip, ipCount)
            }
        }

        userSuspects.forEach { (userId, userCount) ->
            if (!incidentRepository.existsByTypeAndUserAndWindow("brute_force", userId, windowMs)) {
                incidentRepository.upsert(
                    Incident(
                        incidentId = UUID.randomUUID().toString(),
                        type = "brute_force",
                        status = "open",
                        detectedAt = System.currentTimeMillis(),
                        userId = userId,
                        details =
                            mapOf(
                                "detection_type" to "credential_stuffing",
                                "user_id" to userId,
                                "user_attempt_count" to userCount,
                                "window_minutes" to bruteForceWindowMinutes,
                            ),
                        updatedAt = System.currentTimeMillis(),
                    ),
                )
                log.warn("Brute force (credential stuffing) detected for userId={} attempts={}", userId, userCount)
            }
        }
    }

    // ── Rule: Concurrent Sessions ─────────────────────────────────────────────

    private fun detectConcurrentSessions() {
        val windowMs = concurrentSessionsWindowMinutes * 60_000L
        val params =
            MapSqlParameterSource().apply {
                addValue("windowTs", System.currentTimeMillis() - windowMs)
                addValue("threshold", concurrentSessionsThreshold)
            }

        val suspects =
            jdbc.query(
                """
                SELECT
                    sess_user_id AS user_id,
                    uniqExact(sess_remote_addr) AS ip_count
                FROM audit.client_req_log
                WHERE log_timestamp >= fromUnixTimestamp64Milli(:windowTs)
                  AND sess_user_id != ''
                  AND sess_remote_addr != ''
                GROUP BY sess_user_id
                HAVING ip_count >= :threshold
                """.trimIndent(),
                params,
            ) { rs, _ ->
                rs.getString("user_id") to rs.getLong("ip_count")
            }

        suspects.forEach { (userId, ipCount) ->
            if (!incidentRepository.existsByTypeAndUserAndWindow("concurrent_sessions", userId, windowMs)) {
                incidentRepository.upsert(
                    Incident(
                        incidentId = UUID.randomUUID().toString(),
                        type = "concurrent_sessions",
                        status = "open",
                        detectedAt = System.currentTimeMillis(),
                        userId = userId,
                        details =
                            mapOf(
                                "unique_ips" to ipCount,
                                "window_minutes" to concurrentSessionsWindowMinutes,
                            ),
                        updatedAt = System.currentTimeMillis(),
                    ),
                )
                log.warn("Concurrent sessions detected userId={} unique_ips={}", userId, ipCount)
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
                addValue("minThreshold", volumeAnomalyMinThreshold)
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
                  AND current_count >= :minThreshold
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

    // ── Rule: Topic Enumeration ───────────────────────────────────────────────

    private fun detectTopicEnumeration() {
        val windowMs = topicEnumerationWindowMinutes * 60_000L
        val params =
            MapSqlParameterSource().apply {
                addValue("windowTs", System.currentTimeMillis() - windowMs)
                addValue("threshold", topicEnumerationThreshold)
            }

        val suspects =
            jdbc.query(
                """
                SELECT
                    user_id,
                    count(DISTINCT topic) AS failure_count
                FROM audit.subscription_log
                WHERE log_timestamp >= fromUnixTimestamp64Milli(:windowTs)
                  AND user_id != ''
                  AND action = 'CREATE'
                GROUP BY user_id
                HAVING failure_count >= :threshold
                """.trimIndent(),
                params,
            ) { rs, _ -> rs.getString("user_id") to rs.getLong("failure_count") }

        suspects.forEach { (userId, count) ->
            if (!incidentRepository.existsByTypeAndUserAndWindow("topic_enumeration", userId, windowMs)) {
                incidentRepository.upsert(
                    Incident(
                        incidentId = UUID.randomUUID().toString(),
                        type = "topic_enumeration",
                        status = "open",
                        detectedAt = System.currentTimeMillis(),
                        userId = userId,
                        details =
                            mapOf(
                                "subscription_attempts" to count,
                                "window_minutes" to topicEnumerationWindowMinutes,
                            ),
                        updatedAt = System.currentTimeMillis(),
                    ),
                )
                log.warn("Topic enumeration detected userId={} attempts={}", userId, count)
            }
        }
    }

    // ── Rule: Inactive Account Activation ──────────────────────────────────────

    private fun detectInactiveAccountActivation() {
        val inactivityMs = inactiveActivationInactivityDays.toLong() * 24 * 3_600_000L
        val currentHourMs = 3_600_000L

        // Step 1: Get system median for first hour after any login
        val systemMedian =
            jdbc.queryForObject(
                """
                SELECT
                    medianExact(hourly_count) AS system_median
                FROM (
                    SELECT
                        sess_user_id,
                        count() AS hourly_count
                    FROM audit.client_req_log
                    WHERE log_timestamp >= fromUnixTimestamp64Milli(:baselineWindowTs)
                      AND msg_type = 'LOGIN'
                    GROUP BY sess_user_id, toStartOfHour(log_timestamp)
                )
                """.trimIndent(),
                mapOf(
                    "baselineWindowTs" to (System.currentTimeMillis() - 30L * 24 * 3_600_000L),
                ),
                Double::class.java,
            ) ?: 1.0

        val threshold = (systemMedian * inactiveActivationMultiplier).toLong()

        // Step 2: Find users inactive for >30 days
        val params =
            MapSqlParameterSource().apply {
                addValue("inactivityThresholdTs", System.currentTimeMillis() - inactivityMs)
                addValue("currentWindowTs", System.currentTimeMillis() - currentHourMs)
                addValue("requestThreshold", threshold)
            }

        val suspects =
            jdbc.query(
                """
                SELECT
                    c.sess_user_id AS user_id,
                    count() AS request_count,
                    max(a.last_activity) AS last_activity
                FROM audit.client_req_log c
                LEFT JOIN (
                    SELECT user_id, max(log_timestamp) AS last_activity
                    FROM audit.account_log
                    GROUP BY user_id
                ) AS a ON c.sess_user_id = a.user_id
                WHERE c.log_timestamp >= fromUnixTimestamp64Milli(:currentWindowTs)
                  AND c.sess_user_id != ''
                GROUP BY c.sess_user_id
                HAVING a.last_activity < fromUnixTimestamp64Milli(:inactivityThresholdTs)
                  AND request_count >= :requestThreshold
                """.trimIndent(),
                params,
            ) { rs, _ ->
                Triple(
                    rs.getString("user_id"),
                    rs.getLong("request_count"),
                    rs.getTimestamp("last_activity")?.time ?: 0L,
                )
            }

        suspects.forEach { (userId, count, lastActivity) ->
            if (!incidentRepository.existsByTypeAndUserAndWindow("inactive_account_activation", userId, currentHourMs)) {
                incidentRepository.upsert(
                    Incident(
                        incidentId = UUID.randomUUID().toString(),
                        type = "inactive_account_activation",
                        status = "open",
                        detectedAt = System.currentTimeMillis(),
                        userId = userId,
                        details =
                            mapOf(
                                "current_hour_requests" to count,
                                "inactivity_days" to inactiveActivationInactivityDays,
                                "system_median_multiplier" to inactiveActivationMultiplier,
                                "last_activity_ts" to lastActivity,
                            ),
                        updatedAt = System.currentTimeMillis(),
                    ),
                )
                log.warn("Inactive account reactivation detected userId={} requests={}", userId, count)
            }
        }
    }

    // ── Rule: Off-Hours Activity ───────────────────────────────────────────────

    private fun detectOffHoursActivity() {
        val startHour = offHoursStart.split(":")[0].toInt()
        val endHour = offHoursEnd.split(":")[0].toInt()
        val windowMs = 24 * 3_600_000L

        val params =
            MapSqlParameterSource().apply {
                addValue("windowTs", System.currentTimeMillis() - windowMs)
                addValue("startHour", startHour)
                addValue("endHour", endHour)
                addValue("threshold", offHoursThreshold)
                addValue("timezone", offHoursTimeZone)
            }

        val suspects =
            jdbc.query(
                """
                SELECT
                    sess_user_id AS user_id,
                    count() AS request_count
                FROM audit.client_req_log
                WHERE log_timestamp >= fromUnixTimestamp64Milli(:windowTs)
                  AND sess_user_id != ''
                  AND (
                    toHour(toTimeZone(log_timestamp, :timezone)) < :startHour
                    OR toHour(toTimeZone(log_timestamp, :timezone)) >= :endHour
                  )
                GROUP BY sess_user_id
                HAVING request_count >= :threshold
                """.trimIndent(),
                params,
            ) { rs, _ -> rs.getString("user_id") to rs.getLong("request_count") }

        suspects.forEach { (userId, count) ->
            if (!incidentRepository.existsByTypeAndUserAndWindow("off_hours_activity", userId, windowMs)) {
                incidentRepository.upsert(
                    Incident(
                        incidentId = UUID.randomUUID().toString(),
                        type = "off_hours_activity",
                        status = "open",
                        detectedAt = System.currentTimeMillis(),
                        userId = userId,
                        details =
                            mapOf(
                                "request_count" to count,
                                "business_hours" to "$offHoursStart–$offHoursEnd",
                                "timezone" to offHoursTimeZone,
                            ),
                        updatedAt = System.currentTimeMillis(),
                    ),
                )
                log.warn("Off-hours activity detected userId={} requests={}", userId, count)
            }
        }
    }

    // ── Rule: Privilege Escalation Attempt ─────────────────────────────────────

    private fun detectPrivilegeEscalation() {
        val params = MapSqlParameterSource()

        val suspects =
            jdbc.query(
                """
                SELECT
                    user_id,
                    arrayJoin(arrayFilter(x -> x LIKE 'role:%', tags)) AS changed_role
                FROM audit.account_log
                WHERE action = 'UPDATE'
                  AND log_timestamp >= fromUnixTimestamp64Milli(:windowTs)
                  AND user_id != ''
                  AND arrayExists(x -> x LIKE 'role:%', tags)
                """.trimIndent(),
                params.apply {
                    addValue("windowTs", System.currentTimeMillis() - 3_600_000L)
                },
            ) { rs, _ -> rs.getString("user_id") to rs.getString("changed_role") }

        suspects.forEach { (userId, role) ->
            if (!incidentRepository.existsByTypeAndUserAndWindow("privilege_escalation", userId, 0L)) {
                incidentRepository.upsert(
                    Incident(
                        incidentId = UUID.randomUUID().toString(),
                        type = "privilege_escalation",
                        status = "open",
                        detectedAt = System.currentTimeMillis(),
                        userId = userId,
                        details =
                            mapOf(
                                "role_change" to role,
                                "severity" to "high",
                            ),
                        updatedAt = System.currentTimeMillis(),
                    ),
                )
                log.warn("Privilege escalation attempt detected userId={} role={}", userId, role)
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

        val detailsPatch =
            buildMap {
                if (!comment.isNullOrBlank()) put("admin_comment", comment)
            }
        val updated =
            existing.copy(
                status = status,
                details = existing.details + detailsPatch,
                updatedAt = System.currentTimeMillis(),
            )
        incidentRepository.upsert(updated)
        return updated
    }
}