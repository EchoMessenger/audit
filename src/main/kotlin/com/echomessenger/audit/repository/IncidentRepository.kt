package com.echomessenger.audit.repository

import com.echomessenger.audit.domain.Incident
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import java.sql.ResultSet

@Repository
class IncidentRepository(
    @Qualifier("clickHouseJdbcTemplate")
    private val jdbc: NamedParameterJdbcTemplate,
) {
    private val mapper = jacksonObjectMapper()

    fun findAll(
        status: String? = null,
        type: String? = null,
        userId: String? = null,
        limit: Int = 100,
    ): List<Incident> {
        val conditions =
            buildList {
                if (status != null) add("status = :status")
                if (type != null) add("type = :type")
                if (userId != null) add("user_id = :userId")
            }.let { if (it.isEmpty()) "1=1" else it.joinToString(" AND ") }

        val params =
            MapSqlParameterSource().apply {
                addValue("status", status)
                addValue("type", type)
                addValue("userId", userId)
                addValue("limit", limit.coerceIn(1, 1000))
            }

        return jdbc.query(
            """
            SELECT incident_id, type, status, user_id, details,
                   toUnixTimestamp64Milli(detected_at) AS detected_at,
                   toUnixTimestamp64Milli(updated_at)  AS updated_at
            FROM audit.incident_log
            WHERE $conditions
            ORDER BY detected_at DESC
            LIMIT :limit
            """.trimIndent(),
            params,
        ) { rs, _ -> mapIncident(rs) }
    }

    fun findById(incidentId: String): Incident? {
        // Валидируем UUID формат до запроса — toUUID() в ClickHouse бросает исключение
        // для невалидных строк (например "nonexistent-id"), что превращается в 500.
        if (!isValidUuid(incidentId)) return null

        return runCatching {
            jdbc
                .query(
                    """
                    SELECT incident_id, type, status, user_id, details,
                           toUnixTimestamp64Milli(detected_at) AS detected_at,
                           toUnixTimestamp64Milli(updated_at)  AS updated_at
                    FROM audit.incident_log
                    WHERE incident_id = toUUID(:id)
                    LIMIT 1
                    """.trimIndent(),
                    MapSqlParameterSource("id", incidentId),
                ) { rs, _ -> mapIncident(rs) }
                .firstOrNull()
        }.getOrNull()
    }

    private fun isValidUuid(value: String): Boolean =
        runCatching {
            java.util.UUID.fromString(value)
        }.isSuccess

    /**
     * INSERT в ReplacingMergeTree — обновление статуса через замену строки по incident_id.
     * updated_at используется как версия для deduplicate.
     */
    fun upsert(incident: Incident) {
        // ClickHouse не поддерживает JDBC batch bind для сложных типов,
        // используем параметризованный INSERT
        val detailsJson = mapper.writeValueAsString(incident.details)
        val sql =
            """
            INSERT INTO audit.incident_log
                (incident_id, detected_at, type, status, user_id, details, updated_at)
            VALUES
                (:incidentId,
                 fromUnixTimestamp64Milli(:detectedAt),
                 :type, :status, :userId, :details,
                 fromUnixTimestamp64Milli(:updatedAt))
            """.trimIndent()

        jdbc.update(
            sql,
            MapSqlParameterSource().apply {
                addValue("incidentId", incident.incidentId)
                addValue("detectedAt", incident.detectedAt)
                addValue("type", incident.type)
                addValue("status", incident.status)
                addValue("userId", incident.userId)
                addValue("details", detailsJson)
                addValue("updatedAt", incident.updatedAt)
            },
        )
    }

    fun existsByTypeAndUserAndWindow(
        type: String,
        userId: String?,
        windowMs: Long,
    ): Boolean {
        // Строим условие на userId явно — NULL через JDBC не работает как IS NULL в ClickHouse
        val userCondition = if (userId != null) "AND user_id = :userId" else ""
        val params =
            MapSqlParameterSource().apply {
                addValue("type", type)
                if (userId != null) addValue("userId", userId)
                addValue("windowTs", System.currentTimeMillis() - windowMs)
            }
        val count =
            jdbc.queryForObject(
                """
                SELECT count() FROM audit.incident_log
                WHERE type = :type
                  $userCondition
                  AND status = 'open'
                  AND detected_at >= fromUnixTimestamp64Milli(:windowTs)
                """.trimIndent(),
                params,
                Long::class.java,
            ) ?: 0L
        return count > 0
    }

    @Suppress("UNCHECKED_CAST")
    private fun mapIncident(rs: ResultSet): Incident {
        val details =
            try {
                mapper.readValue(rs.getString("details") ?: "{}", Map::class.java) as Map<String, Any?>
            } catch (_: Exception) {
                emptyMap()
            }
        return Incident(
            incidentId = rs.getString("incident_id"),
            type = rs.getString("type"),
            status = rs.getString("status"),
            detectedAt = rs.getLong("detected_at"),
            userId = rs.getString("user_id")?.takeIf { it.isNotBlank() },
            details = details,
            updatedAt = rs.getLong("updated_at"),
        )
    }
}
