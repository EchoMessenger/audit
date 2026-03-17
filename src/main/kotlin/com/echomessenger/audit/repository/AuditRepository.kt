package com.echomessenger.audit.repository

import com.echomessenger.audit.domain.AuditEvent
import com.echomessenger.audit.domain.CursorCodec
import com.echomessenger.audit.domain.CursorPage
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import java.sql.ResultSet

@Repository
class AuditRepository(
    private val jdbc: NamedParameterJdbcTemplate,
) {
    /**
     * GET /audit/events — поиск по всем таблицам.
     *
     * ClickHouse JDBC 0.7.x не поддерживает именованные параметры (:param) и
     * подзапросы с UNION ALL через PreparedStatement одновременно.
     * Решение: запрашиваем каждую таблицу отдельно, объединяем и сортируем в памяти.
     * Для типичных лимитов (100-1000) это negligible overhead.
     */
    fun findEvents(
        userId: String? = null,
        actorUserId: String? = null,
        topicId: String? = null,
        eventType: String? = null,
        fromTs: Long? = null,
        toTs: Long? = null,
        status: String? = null,
        cursor: String? = null,
        limit: Int = 100,
    ): CursorPage<AuditEvent> {
        val (cursorTs, _) = if (cursor != null) CursorCodec.decode(cursor) else null to null
        val effectiveLimit = limit.coerceIn(1, 1000)
        val msgTypes = eventType?.let { mapEventTypeToMsgTypes(it) }

        // Запрашиваем каждую таблицу отдельно — обходим ограничение JDBC с UNION ALL
        val clientRows =
            if (topicId == null) {
                queryClientReqLog(userId ?: actorUserId, msgTypes, fromTs, toTs, cursorTs, effectiveLimit + 1)
            } else {
                emptyList()
            }

        val messageRows =
            if (msgTypes == null || msgTypes.any { it in listOf("PUB", "EDIT", "DEL", "HDEL") }) {
                queryMessageLog(userId ?: actorUserId, topicId, msgTypes, fromTs, toTs, cursorTs, effectiveLimit + 1)
            } else {
                emptyList()
            }

        // Объединяем и сортируем в памяти по timestamp DESC
        val combined =
            (clientRows + messageRows)
                .sortedByDescending { it.timestamp }
                .take(effectiveLimit + 1)

        val hasMore = combined.size > effectiveLimit
        val page = if (hasMore) combined.dropLast(1) else combined
        val nextCursor =
            if (hasMore && page.isNotEmpty()) {
                val last = page.last()
                CursorCodec.encode(last.timestamp, last.eventId)
            } else {
                null
            }

        return CursorPage(data = page, nextCursor = nextCursor, hasMore = hasMore)
    }

    private fun queryClientReqLog(
        userId: String?,
        msgTypes: List<String>?,
        fromTs: Long?,
        toTs: Long?,
        cursorTs: Long?,
        limit: Int,
    ): List<AuditEvent> {
        val conditions =
            buildList {
                if (userId != null) add("sess_user_id = :userId")
                if (fromTs != null) add("req_ts >= fromUnixTimestamp64Milli(:fromTs)")
                if (toTs != null) add("req_ts <= fromUnixTimestamp64Milli(:toTs)")
                if (cursorTs != null) add("req_ts < fromUnixTimestamp64Milli(:cursorTs)")
                if (msgTypes != null) add("msg_type IN (:msgTypes)")
            }.let { if (it.isEmpty()) "1=1" else it.joinToString(" AND ") }

        val params =
            MapSqlParameterSource().apply {
                addValue("userId", userId)
                addValue("fromTs", fromTs)
                addValue("toTs", toTs)
                addValue("cursorTs", cursorTs)
                addValue("limit", limit)
                if (msgTypes != null) addValue("msgTypes", msgTypes)
            }

        return jdbc.query(
            """
            SELECT
                log_id                                          AS event_id,
                if(sess_auth_level > 0, 'success', 'failure')  AS status,
                toUnixTimestamp64Milli(req_ts)                  AS timestamp,
                sess_user_id                                    AS user_id,
                NULL                                            AS topic_id,
                msg_type,
                client_ip                                       AS ip,
                user_agent,
                sess_device_id                                  AS device_id,
                sess_session_id
            FROM audit.client_req_log
            WHERE $conditions
            ORDER BY req_ts DESC
            LIMIT :limit
            """.trimIndent(),
            params,
        ) { rs, _ -> mapRowToAuditEvent(rs) }
    }

    private fun queryMessageLog(
        userId: String?,
        topicId: String?,
        msgTypes: List<String>?,
        fromTs: Long?,
        toTs: Long?,
        cursorTs: Long?,
        limit: Int,
    ): List<AuditEvent> {
        val conditions =
            buildList {
                if (userId != null) add("usr_id = :userId")
                if (topicId != null) add("topic_id = :topicId")
                if (fromTs != null) add("msg_ts >= fromUnixTimestamp64Milli(:fromTs)")
                if (toTs != null) add("msg_ts <= fromUnixTimestamp64Milli(:toTs)")
                if (cursorTs != null) add("msg_ts < fromUnixTimestamp64Milli(:cursorTs)")
                if (msgTypes != null) add("msg_type IN (:msgTypes)")
            }.let { if (it.isEmpty()) "1=1" else it.joinToString(" AND ") }

        val params =
            MapSqlParameterSource().apply {
                addValue("userId", userId)
                addValue("topicId", topicId)
                addValue("fromTs", fromTs)
                addValue("toTs", toTs)
                addValue("cursorTs", cursorTs)
                addValue("limit", limit)
                if (msgTypes != null) addValue("msgTypes", msgTypes)
            }

        return jdbc.query(
            """
            SELECT
                toString(seq_id)                    AS event_id,
                'success'                           AS status,
                toUnixTimestamp64Milli(msg_ts)      AS timestamp,
                usr_id                              AS user_id,
                topic_id,
                msg_type,
                ''                                  AS ip,
                ''                                  AS user_agent,
                ''                                  AS device_id,
                ''                                  AS sess_session_id
            FROM audit.message_log
            WHERE $conditions
            ORDER BY msg_ts DESC
            LIMIT :limit
            """.trimIndent(),
            params,
        ) { rs, _ -> mapRowToAuditEvent(rs) }
    }

    fun findEventById(eventId: String): AuditEvent? {
        // Ищем сначала в client_req_log (UUID), потом в message_log (numeric seq_id)
        val fromClient =
            runCatching {
                jdbc.query(
                    """
                    SELECT
                        log_id                                              AS event_id,
                        toUnixTimestamp64Milli(req_ts)                      AS timestamp,
                        sess_user_id                                        AS user_id,
                        if(sess_auth_level > 0, 'success', 'failure')       AS status,
                        NULL                                                AS topic_id,
                        msg_type,
                        client_ip                                           AS ip,
                        user_agent,
                        sess_device_id                                      AS device_id,
                        sess_session_id
                    FROM audit.client_req_log
                    WHERE log_id = toUUID(:eventId)
                    LIMIT 1
                    """.trimIndent(),
                    MapSqlParameterSource("eventId", eventId),
                ) { rs, _ -> mapRowToAuditEvent(rs) }
            }.getOrElse { emptyList() }

        if (fromClient.isNotEmpty()) return fromClient.first()

        return jdbc
            .query(
                """
                SELECT
                    toString(seq_id)                    AS event_id,
                    toUnixTimestamp64Milli(msg_ts)      AS timestamp,
                    usr_id                              AS user_id,
                    'success'                           AS status,
                    topic_id,
                    msg_type,
                    ''                                  AS ip,
                    ''                                  AS user_agent,
                    ''                                  AS device_id,
                    ''                                  AS sess_session_id
                FROM audit.message_log
                WHERE toString(seq_id) = :eventId
                LIMIT 1
                """.trimIndent(),
                MapSqlParameterSource("eventId", eventId),
            ) { rs, _ -> mapRowToAuditEvent(rs) }
            .firstOrNull()
    }

    /**
     * GET /audit/auth-events — только LOGIN, HI, BYE, REG из client_req_log
     */
    fun findAuthEvents(
        userId: String? = null,
        fromTs: Long? = null,
        toTs: Long? = null,
        cursor: String? = null,
        limit: Int = 100,
    ): CursorPage<AuditEvent> {
        val (cursorTs, _) = if (cursor != null) CursorCodec.decode(cursor) else null to null
        val effectiveLimit = limit.coerceIn(1, 1000)

        val conditions =
            buildList {
                add("msg_type IN ('LOGIN', 'HI', 'BYE', 'REG')")
                if (userId != null) add("sess_user_id = :userId")
                if (fromTs != null) add("req_ts >= fromUnixTimestamp64Milli(:fromTs)")
                if (toTs != null) add("req_ts <= fromUnixTimestamp64Milli(:toTs)")
                if (cursorTs != null) add("req_ts < fromUnixTimestamp64Milli(:cursorTs)")
            }

        val params =
            MapSqlParameterSource().apply {
                addValue("userId", userId)
                addValue("fromTs", fromTs)
                addValue("toTs", toTs)
                addValue("cursorTs", cursorTs)
                addValue("limit", effectiveLimit + 1)
            }

        val rows =
            jdbc.query(
                """
                SELECT
                    log_id                                          AS event_id,
                    msg_type,
                    toUnixTimestamp64Milli(req_ts)                  AS timestamp,
                    sess_user_id                                    AS user_id,
                    NULL                                            AS topic_id,
                    if(sess_auth_level > 0, 'success', 'failure')  AS status,
                    client_ip                                       AS ip,
                    user_agent,
                    sess_device_id                                  AS device_id,
                    sess_session_id
                FROM audit.client_req_log
                WHERE ${conditions.joinToString(" AND ")}
                ORDER BY req_ts DESC
                LIMIT :limit
                """.trimIndent(),
                params,
            ) { rs, _ -> mapRowToAuditEvent(rs) }

        val hasMore = rows.size > effectiveLimit
        val page = if (hasMore) rows.dropLast(1) else rows
        val nextCursor =
            if (hasMore && page.isNotEmpty()) {
                CursorCodec.encode(page.last().timestamp, page.last().eventId)
            } else {
                null
            }

        return CursorPage(data = page, nextCursor = nextCursor, hasMore = hasMore)
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private fun mapRowToAuditEvent(rs: ResultSet): AuditEvent {
        val msgType = rs.getString("msg_type") ?: ""
        val userId = rs.getString("user_id")?.takeIf { it.isNotBlank() }
        return AuditEvent(
            eventId = rs.getString("event_id"),
            eventType = mapMsgTypeToEventType(msgType),
            timestamp = rs.getLong("timestamp"),
            userId = userId,
            actorUserId = userId,
            topicId = rs.getString("topic_id")?.takeIf { it.isNotBlank() },
            status = rs.getString("status"),
            metadata =
                buildMap {
                    if (msgType.isNotBlank()) put("msg_type", msgType)
                    rs
                        .getString("sess_session_id")
                        ?.takeIf { it.isNotBlank() }
                        ?.let { put("session_id", it) }
                },
            ip = rs.getString("ip")?.takeIf { it.isNotBlank() },
            userAgent = rs.getString("user_agent")?.takeIf { it.isNotBlank() },
            deviceId = rs.getString("device_id")?.takeIf { it.isNotBlank() },
        )
    }

    companion object {
        fun mapMsgTypeToEventType(msgType: String): String =
            when (msgType.uppercase()) {
                "LOGIN" -> "auth.login"
                "HI" -> "auth.session_start"
                "BYE" -> "auth.logout"
                "REG" -> "auth.register"
                "PUB" -> "message.create"
                "EDIT" -> "message.edit"
                "DEL" -> "message.delete"
                "HDEL" -> "message.hard_delete"
                "CREATE" -> "topic.create"
                "DELETE" -> "topic.delete"
                "JOIN" -> "subscription.join"
                "LEAVE" -> "subscription.leave"
                "ROLE" -> "subscription.role_change"
                else -> "unknown.$msgType"
            }

        fun mapEventTypeToMsgTypes(eventType: String): List<String>? =
            when (eventType) {
                "auth.login" -> listOf("LOGIN")
                "auth.session_start" -> listOf("HI")
                "auth.logout" -> listOf("BYE")
                "auth.register" -> listOf("REG")
                "message.create" -> listOf("PUB")
                "message.edit" -> listOf("EDIT")
                "message.delete" -> listOf("DEL")
                "message.hard_delete" -> listOf("HDEL")
                "topic.create" -> listOf("CREATE")
                "topic.delete" -> listOf("DELETE")
                "subscription.join" -> listOf("JOIN")
                "subscription.leave" -> listOf("LEAVE")
                "subscription.role_change" -> listOf("ROLE")
                else -> null
            }
    }
}
