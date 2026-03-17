package com.echomessenger.audit.repository

import com.echomessenger.audit.domain.AuditEvent
import com.echomessenger.audit.domain.CursorCodec
import com.echomessenger.audit.domain.CursorPage
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import java.sql.ResultSet

@Repository
class UserTimelineRepository(
    private val jdbc: NamedParameterJdbcTemplate,
) {
    /**
     * Объединяет события из трёх таблиц для полной хронологии пользователя.
     *
     * Используем три отдельных запроса вместо UNION ALL в подзапросе —
     * ClickHouse JDBC 0.7.x не поддерживает параметры (?/named) внутри подзапросов UNION ALL.
     * Результаты объединяем и сортируем в памяти.
     */
    fun getTimeline(
        userId: String,
        fromTs: Long? = null,
        toTs: Long? = null,
        cursor: String? = null,
        limit: Int = 100,
    ): CursorPage<AuditEvent> {
        val effectiveLimit = limit.coerceIn(1, 500)
        val (cursorTs, _) = if (cursor != null) CursorCodec.decode(cursor) else null to null

        val clientRows = queryClientReqLog(userId, fromTs, toTs, cursorTs, effectiveLimit + 1)
        val messageRows = queryMessageLog(userId, fromTs, toTs, cursorTs, effectiveLimit + 1)
        val subscriptionRows = querySubscriptionLog(userId, fromTs, toTs, cursorTs, effectiveLimit + 1)

        val combined =
            (clientRows + messageRows + subscriptionRows)
                .sortedByDescending { it.timestamp }
                .take(effectiveLimit + 1)

        val hasMore = combined.size > effectiveLimit
        val page = if (hasMore) combined.dropLast(1) else combined
        val nextCursor =
            if (hasMore && page.isNotEmpty()) {
                CursorCodec.encode(page.last().timestamp, page.last().eventId)
            } else {
                null
            }

        return CursorPage(data = page, nextCursor = nextCursor, hasMore = hasMore)
    }

    private fun queryClientReqLog(
        userId: String,
        fromTs: Long?,
        toTs: Long?,
        cursorTs: Long?,
        limit: Int,
    ): List<AuditEvent> {
        val conditions =
            buildList {
                add("sess_user_id = :userId")
                if (fromTs != null) add("req_ts >= fromUnixTimestamp64Milli(:fromTs)")
                if (toTs != null) add("req_ts <= fromUnixTimestamp64Milli(:toTs)")
                if (cursorTs != null) add("req_ts < fromUnixTimestamp64Milli(:cursorTs)")
            }.joinToString(" AND ")

        val params =
            MapSqlParameterSource()
                .addValue("userId", userId)
                .addValue("fromTs", fromTs)
                .addValue("toTs", toTs)
                .addValue("cursorTs", cursorTs)
                .addValue("limit", limit)

        return jdbc.query(
            """
            SELECT
                log_id                                          AS event_id,
                'auth.' || lower(msg_type)                      AS event_type,
                toUnixTimestamp64Milli(req_ts)                  AS ts,
                sess_user_id                                    AS user_id,
                if(sess_auth_level > 0, 'success', 'failure')  AS status,
                NULL                                            AS topic_id,
                sess_session_id,
                sess_device_id                                  AS device_id,
                client_ip                                       AS ip
            FROM audit.client_req_log
            WHERE $conditions
            ORDER BY req_ts DESC
            LIMIT :limit
            """.trimIndent(),
            params,
        ) { rs, _ -> mapClientRow(rs) }
    }

    private fun queryMessageLog(
        userId: String,
        fromTs: Long?,
        toTs: Long?,
        cursorTs: Long?,
        limit: Int,
    ): List<AuditEvent> {
        val conditions =
            buildList {
                add("usr_id = :userId")
                if (fromTs != null) add("msg_ts >= fromUnixTimestamp64Milli(:fromTs)")
                if (toTs != null) add("msg_ts <= fromUnixTimestamp64Milli(:toTs)")
                if (cursorTs != null) add("msg_ts < fromUnixTimestamp64Milli(:cursorTs)")
            }.joinToString(" AND ")

        val params =
            MapSqlParameterSource()
                .addValue("userId", userId)
                .addValue("fromTs", fromTs)
                .addValue("toTs", toTs)
                .addValue("cursorTs", cursorTs)
                .addValue("limit", limit)

        return jdbc.query(
            """
            SELECT
                toString(seq_id)                    AS event_id,
                'message.' || lower(msg_type)       AS event_type,
                toUnixTimestamp64Milli(msg_ts)      AS ts,
                usr_id                              AS user_id,
                'success'                           AS status,
                topic_id,
                ''                                  AS sess_session_id,
                ''                                  AS device_id,
                ''                                  AS ip
            FROM audit.message_log
            WHERE $conditions
            ORDER BY msg_ts DESC
            LIMIT :limit
            """.trimIndent(),
            params,
        ) { rs, _ -> mapMessageRow(rs) }
    }

    private fun querySubscriptionLog(
        userId: String,
        fromTs: Long?,
        toTs: Long?,
        cursorTs: Long?,
        limit: Int,
    ): List<AuditEvent> {
        val conditions =
            buildList {
                add("user_id = :userId")
                if (fromTs != null) add("event_ts >= fromUnixTimestamp64Milli(:fromTs)")
                if (toTs != null) add("event_ts <= fromUnixTimestamp64Milli(:toTs)")
                if (cursorTs != null) add("event_ts < fromUnixTimestamp64Milli(:cursorTs)")
            }.joinToString(" AND ")

        val params =
            MapSqlParameterSource()
                .addValue("userId", userId)
                .addValue("fromTs", fromTs)
                .addValue("toTs", toTs)
                .addValue("cursorTs", cursorTs)
                .addValue("limit", limit)

        return jdbc.query(
            """
            SELECT
                toString(rowNumberInAllBlocks())    AS event_id,
                'subscription.' || lower(action)   AS event_type,
                toUnixTimestamp64Milli(event_ts)   AS ts,
                user_id,
                'success'                          AS status,
                topic_id,
                ''                                 AS sess_session_id,
                ''                                 AS device_id,
                ''                                 AS ip
            FROM audit.subscription_log
            WHERE $conditions
            ORDER BY event_ts DESC
            LIMIT :limit
            """.trimIndent(),
            params,
        ) { rs, _ -> mapSubscriptionRow(rs) }
    }

    private fun mapClientRow(rs: ResultSet) =
        AuditEvent(
            eventId = rs.getString("event_id"),
            eventType = rs.getString("event_type"),
            timestamp = rs.getLong("ts"),
            userId = rs.getString("user_id")?.takeIf { it.isNotBlank() },
            actorUserId = rs.getString("user_id")?.takeIf { it.isNotBlank() },
            topicId = null,
            status = rs.getString("status"),
            metadata =
                buildMap {
                    rs.getString("sess_session_id")?.takeIf { it.isNotBlank() }?.let { put("session_id", it) }
                    rs.getString("device_id")?.takeIf { it.isNotBlank() }?.let { put("device_id", it) }
                },
            ip = rs.getString("ip")?.takeIf { it.isNotBlank() },
        )

    private fun mapMessageRow(rs: ResultSet) =
        AuditEvent(
            eventId = rs.getString("event_id"),
            eventType = rs.getString("event_type"),
            timestamp = rs.getLong("ts"),
            userId = rs.getString("user_id")?.takeIf { it.isNotBlank() },
            actorUserId = rs.getString("user_id")?.takeIf { it.isNotBlank() },
            topicId = rs.getString("topic_id")?.takeIf { it.isNotBlank() },
            status = rs.getString("status"),
            metadata = emptyMap(),
        )

    private fun mapSubscriptionRow(rs: ResultSet) =
        AuditEvent(
            eventId = rs.getString("event_id"),
            eventType = rs.getString("event_type"),
            timestamp = rs.getLong("ts"),
            userId = rs.getString("user_id")?.takeIf { it.isNotBlank() },
            actorUserId = rs.getString("user_id")?.takeIf { it.isNotBlank() },
            topicId = rs.getString("topic_id")?.takeIf { it.isNotBlank() },
            status = rs.getString("status"),
            metadata = emptyMap(),
        )
}
