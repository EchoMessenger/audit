package com.echomessenger.audit.repository

import com.echomessenger.audit.domain.AuditEvent
import com.echomessenger.audit.domain.CursorCodec
import com.echomessenger.audit.domain.CursorPage
import com.echomessenger.audit.support.AuditEventMapping
import com.echomessenger.audit.support.AuditEventSort
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import java.sql.ResultSet
import java.util.UUID

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
        val branchLimit = minOf(500, maxOf(effectiveLimit + 1, 48))
        val (cursorTs, cursorLogId) =
            if (cursor != null) {
                val (ts, id) = CursorCodec.decode(cursor)
                try {
                    UUID.fromString(id)
                } catch (_: IllegalArgumentException) {
                    throw IllegalArgumentException("Invalid cursor log id")
                }
                ts to id
            } else {
                null to null
            }

        val clientRows = queryClientReqLog(userId, fromTs, toTs, cursorTs, cursorLogId, branchLimit)
        val messageRows = queryMessageLog(userId, fromTs, toTs, cursorTs, cursorLogId, branchLimit)
        val subscriptionRows = querySubscriptionLog(userId, fromTs, toTs, cursorTs, cursorLogId, branchLimit)

        val combined =
            (clientRows + messageRows + subscriptionRows)
                .sortedWith(AuditEventSort.DESC)
                .distinctBy { it.eventId }
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
        cursorLogId: String?,
        limit: Int,
    ): List<AuditEvent> {
        val conditions =
            buildList {
                add("sess_user_id = :userId")
                if (fromTs != null) add("log_timestamp >= fromUnixTimestamp64Milli(:fromTs)")
                if (toTs != null) add("log_timestamp <= fromUnixTimestamp64Milli(:toTs)")
                if (cursorTs != null && cursorLogId != null) {
                    add("(log_timestamp, log_id) < (fromUnixTimestamp64Milli(:cursorTs), toUUID(:cursorLogId))")
                }
            }.joinToString(" AND ")

        val params =
            MapSqlParameterSource()
                .addValue("userId", userId)
                .addValue("fromTs", fromTs)
                .addValue("toTs", toTs)
                .addValue("cursorTs", cursorTs)
                .addValue("cursorLogId", cursorLogId)
                .addValue("limit", limit)

        return jdbc.query(
            """
            SELECT
                toString(log_id) AS event_id,
                msg_type,
                toUnixTimestamp64Milli(log_timestamp) AS timestamp,
                sess_user_id AS user_id,
                if(sess_auth_level != '' AND sess_auth_level != '0',
                   'success', 'failure') AS status,
                msg_topic AS topic_id,
                sess_session_id,
                sub_topic,
                get_what,
                set_topic,
                del_what,
                del_user_id,
                sess_remote_addr AS ip,
                sess_user_agent AS user_agent,
                sess_device_id AS device_id
            FROM audit.client_req_log
            WHERE $conditions
            ORDER BY log_timestamp DESC, log_id DESC
            LIMIT :limit
            """.trimIndent(),
            params,
        ) { rs, _ -> AuditEventMapping.mapAuditEventFromResultSet(rs) }
    }

    private fun queryMessageLog(
        userId: String,
        fromTs: Long?,
        toTs: Long?,
        cursorTs: Long?,
        cursorLogId: String?,
        limit: Int,
    ): List<AuditEvent> {
        val conditions =
            buildList {
                add("msg_from_user_id = :userId")
                if (fromTs != null) add("log_timestamp >= fromUnixTimestamp64Milli(:fromTs)")
                if (toTs != null) add("log_timestamp <= fromUnixTimestamp64Milli(:toTs)")
                if (cursorTs != null && cursorLogId != null) {
                    add("(log_timestamp, log_id) < (fromUnixTimestamp64Milli(:cursorTs), toUUID(:cursorLogId))")
                }
            }.joinToString(" AND ")

        val params =
            MapSqlParameterSource()
                .addValue("userId", userId)
                .addValue("fromTs", fromTs)
                .addValue("toTs", toTs)
                .addValue("cursorTs", cursorTs)
                .addValue("cursorLogId", cursorLogId)
                .addValue("limit", limit)

        return jdbc.query(
            """
            SELECT
                toString(log_id) AS event_id,
                CASE toString(action)
                    WHEN 'CREATE' THEN 'PUB'
                    WHEN 'UPDATE' THEN 'EDIT'
                    WHEN 'DELETE' THEN 'DEL'
                    ELSE toString(action)
                END                            AS msg_type,
                toUnixTimestamp64Milli(log_timestamp) AS timestamp,
                msg_from_user_id               AS user_id,
                'success'                       AS status,
                msg_topic                      AS topic_id,
                ''                             AS sess_session_id,
                NULL                           AS sub_topic,
                NULL                           AS get_what,
                NULL                           AS set_topic,
                NULL                           AS del_what,
                NULL                           AS del_user_id,
                ''                             AS ip,
                ''                             AS user_agent,
                ''                             AS device_id
            FROM audit.message_log
            WHERE $conditions
            ORDER BY log_timestamp DESC, log_id DESC
            LIMIT :limit
            """.trimIndent(),
            params,
        ) { rs, _ -> AuditEventMapping.mapAuditEventFromResultSet(rs) }
    }

    private fun querySubscriptionLog(
        userId: String,
        fromTs: Long?,
        toTs: Long?,
        cursorTs: Long?,
        cursorLogId: String?,
        limit: Int,
    ): List<AuditEvent> {
        val conditions =
            buildList {
                add("user_id = :userId")
                if (fromTs != null) add("log_timestamp >= fromUnixTimestamp64Milli(:fromTs)")
                if (toTs != null) add("log_timestamp <= fromUnixTimestamp64Milli(:toTs)")
                if (cursorTs != null && cursorLogId != null) {
                    add("(log_timestamp, log_id) < (fromUnixTimestamp64Milli(:cursorTs), toUUID(:cursorLogId))")
                }
            }.joinToString(" AND ")

        val params =
            MapSqlParameterSource()
                .addValue("userId", userId)
                .addValue("fromTs", fromTs)
                .addValue("toTs", toTs)
                .addValue("cursorTs", cursorTs)
                .addValue("cursorLogId", cursorLogId)
                .addValue("limit", limit)

        return jdbc.query(
            """
            SELECT
                toString(log_id) AS event_id,
                toString(action) AS action,
                toUnixTimestamp64Milli(log_timestamp) AS ts,
                user_id,
                topic AS topic_id
            FROM audit.subscription_log
            WHERE $conditions
            ORDER BY log_timestamp DESC, log_id DESC
            LIMIT :limit
            """.trimIndent(),
            params,
        ) { rs, _ -> mapSubscriptionRow(rs) }
    }

    private fun mapSubscriptionRow(rs: ResultSet): AuditEvent {
        val action = rs.getString("action") ?: ""
        val uid = rs.getString("user_id")?.takeIf { it.isNotBlank() }
        return AuditEvent(
            eventId = rs.getString("event_id"),
            eventType = AuditEventMapping.subscriptionDbActionToEventType(action),
            timestamp = rs.getLong("ts"),
            userId = uid,
            actorUserId = uid,
            topicId = rs.getString("topic_id")?.takeIf { it.isNotBlank() },
            status = "success",
            metadata = emptyMap(),
        )
    }
}
