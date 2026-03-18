package com.echomessenger.audit.repository

import com.echomessenger.audit.domain.AuditEvent
import com.echomessenger.audit.domain.CursorCodec
import com.echomessenger.audit.domain.CursorPage
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import java.sql.ResultSet

@Repository
class UserTimelineRepository(
    @Qualifier("clickHouseJdbcTemplate")
    private val jdbc: NamedParameterJdbcTemplate,
) {
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

        val combined = (clientRows + messageRows + subscriptionRows)
            .sortedByDescending { it.timestamp }
            .take(effectiveLimit + 1)

        val hasMore = combined.size > effectiveLimit
        val page = if (hasMore) combined.dropLast(1) else combined
        val nextCursor = if (hasMore && page.isNotEmpty()) {
            CursorCodec.encode(page.last().timestamp, page.last().eventId)
        } else null

        return CursorPage(data = page, nextCursor = nextCursor, hasMore = hasMore)
    }

    private fun queryClientReqLog(
        userId: String, fromTs: Long?, toTs: Long?, cursorTs: Long?, limit: Int,
    ): List<AuditEvent> {
        val conditions = buildList {
            add("sess_user_id = :userId")
            if (fromTs != null) add("log_timestamp >= fromUnixTimestamp64Milli(:fromTs)")
            if (toTs != null) add("log_timestamp <= fromUnixTimestamp64Milli(:toTs)")
            if (cursorTs != null) add("log_timestamp < fromUnixTimestamp64Milli(:cursorTs)")
        }.joinToString(" AND ")

        val params = MapSqlParameterSource()
            .addValue("userId", userId)
            .addValue("fromTs", fromTs)
            .addValue("toTs", toTs)
            .addValue("cursorTs", cursorTs)
            .addValue("limit", limit)

        return jdbc.query(
            """
            SELECT
                toString(log_id) AS event_id,
                'auth.' || lower(msg_type) AS event_type,
                toUnixTimestamp64Milli(log_timestamp) AS ts,
                sess_user_id AS user_id,
                if(sess_auth_level != '' AND sess_auth_level != '0',
                   'success', 'failure') AS status,
                msg_topic AS topic_id,
                sess_session_id,
                sess_device_id AS device_id,
                sess_remote_addr AS ip
            FROM audit.client_req_log
            WHERE $conditions
            ORDER BY log_timestamp DESC
            LIMIT :limit
            """.trimIndent(),
            params,
        ) { rs, _ -> mapClientRow(rs) }
    }

    private fun queryMessageLog(
        userId: String, fromTs: Long?, toTs: Long?, cursorTs: Long?, limit: Int,
    ): List<AuditEvent> {
        val conditions = buildList {
            add("msg_from_user_id = :userId")
            if (fromTs != null) add("log_timestamp >= fromUnixTimestamp64Milli(:fromTs)")
            if (toTs != null) add("log_timestamp <= fromUnixTimestamp64Milli(:toTs)")
            if (cursorTs != null) add("log_timestamp < fromUnixTimestamp64Milli(:cursorTs)")
        }.joinToString(" AND ")

        val params = MapSqlParameterSource()
            .addValue("userId", userId)
            .addValue("fromTs", fromTs)
            .addValue("toTs", toTs)
            .addValue("cursorTs", cursorTs)
            .addValue("limit", limit)

        return jdbc.query(
            """
            SELECT
                toString(log_id) AS event_id,
                CASE toString(action)
                    WHEN 'CREATE' THEN 'message.create'
                    WHEN 'UPDATE' THEN 'message.update'
                    WHEN 'DELETE' THEN 'message.delete'
                    ELSE 'message.' || lower(toString(action))
                END AS event_type,
                toUnixTimestamp64Milli(log_timestamp) AS ts,
                msg_from_user_id AS user_id,
                'success' AS status,
                msg_topic AS topic_id,
                '' AS sess_session_id,
                '' AS device_id,
                '' AS ip
            FROM audit.message_log
            WHERE $conditions
            ORDER BY log_timestamp DESC
            LIMIT :limit
            """.trimIndent(),
            params,
        ) { rs, _ -> mapMessageRow(rs) }
    }

    private fun querySubscriptionLog(
        userId: String, fromTs: Long?, toTs: Long?, cursorTs: Long?, limit: Int,
    ): List<AuditEvent> {
        val conditions = buildList {
            add("user_id = :userId")
            if (fromTs != null) add("log_timestamp >= fromUnixTimestamp64Milli(:fromTs)")
            if (toTs != null) add("log_timestamp <= fromUnixTimestamp64Milli(:toTs)")
            if (cursorTs != null) add("log_timestamp < fromUnixTimestamp64Milli(:cursorTs)")
        }.joinToString(" AND ")

        val params = MapSqlParameterSource()
            .addValue("userId", userId)
            .addValue("fromTs", fromTs)
            .addValue("toTs", toTs)
            .addValue("cursorTs", cursorTs)
            .addValue("limit", limit)

        return jdbc.query(
            """
            SELECT
                toString(log_id) AS event_id,
                'subscription.' || lower(toString(action)) AS event_type,
                toUnixTimestamp64Milli(log_timestamp) AS ts,
                user_id,
                'success' AS status,
                topic AS topic_id,
                '' AS sess_session_id,
                '' AS device_id,
                '' AS ip
            FROM audit.subscription_log
            WHERE $conditions
            ORDER BY log_timestamp DESC
            LIMIT :limit
            """.trimIndent(),
            params,
        ) { rs, _ -> mapSubscriptionRow(rs) }
    }

    private fun mapClientRow(rs: ResultSet) = AuditEvent(
        eventId = rs.getString("event_id"),
        eventType = rs.getString("event_type"),
        timestamp = rs.getLong("ts"),
        userId = rs.getString("user_id")?.takeIf { it.isNotBlank() },
        actorUserId = rs.getString("user_id")?.takeIf { it.isNotBlank() },
        topicId = rs.getString("topic_id")?.takeIf { it.isNotBlank() },
        status = rs.getString("status"),
        metadata = buildMap {
            rs.getString("sess_session_id")?.takeIf { it.isNotBlank() }?.let { put("session_id", it) }
            rs.getString("device_id")?.takeIf { it.isNotBlank() }?.let { put("device_id", it) }
        },
        ip = rs.getString("ip")?.takeIf { it.isNotBlank() },
    )

    private fun mapMessageRow(rs: ResultSet) = AuditEvent(
        eventId = rs.getString("event_id"),
        eventType = rs.getString("event_type"),
        timestamp = rs.getLong("ts"),
        userId = rs.getString("user_id")?.takeIf { it.isNotBlank() },
        actorUserId = rs.getString("user_id")?.takeIf { it.isNotBlank() },
        topicId = rs.getString("topic_id")?.takeIf { it.isNotBlank() },
        status = rs.getString("status"),
        metadata = emptyMap(),
    )

    private fun mapSubscriptionRow(rs: ResultSet) = AuditEvent(
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