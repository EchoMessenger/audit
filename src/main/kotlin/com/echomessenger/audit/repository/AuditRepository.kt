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
import java.util.UUID

@Repository
class AuditRepository(
    @Qualifier("clickHouseJdbcTemplate")
    private val jdbc: NamedParameterJdbcTemplate,
) {
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
        if (eventType != null && AuditEventMapping.mapEventTypeToMsgTypes(eventType) == null) {
            throw IllegalArgumentException(
                "Unsupported eventType for unified feed: $eventType. Use a supported type or omit the filter.",
            )
        }

        val (cursorTs, cursorLogId) = decodeCursor(cursor)

        val effectiveLimit = limit.coerceIn(1, 1000)
        val msgTypes = eventType?.let { AuditEventMapping.mapEventTypeToMsgTypes(it) }
        val uid = userId ?: actorUserId

        // Два потока (client + message) при фильтре по пользователю: стабильный merge в памяти (cap 10k + 10k).
        if (topicId == null && uid != null) {
            return findEventsMergedInMemory(
                userId = uid,
                msgTypes = msgTypes,
                fromTs = fromTs,
                toTs = toTs,
                status = status,
                cursorTs = cursorTs,
                cursorLogId = cursorLogId,
                effectiveLimit = effectiveLimit,
            )
        }

        val branchLimit = minOf(1000, maxOf(effectiveLimit + 1, 48))

        val clientRows =
            if (topicId == null) {
                queryClientReqLog(
                    uid,
                    msgTypes,
                    fromTs,
                    toTs,
                    cursorTs,
                    cursorLogId,
                    branchLimit,
                )
            } else {
                emptyList()
            }

        val messageRows =
            if (msgTypes == null || msgTypes.any { it in listOf("PUB", "EDIT", "DEL", "HDEL") }) {
                queryMessageLog(
                    uid,
                    topicId,
                    msgTypes,
                    fromTs,
                    toTs,
                    cursorTs,
                    cursorLogId,
                    branchLimit,
                )
            } else {
                emptyList()
            }

        val combined =
            (clientRows + messageRows)
                .filter { ev -> status?.let { s -> ev.status == s } ?: true }
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

    private fun decodeCursor(cursor: String?): Pair<Long?, String?> =
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

    private fun findEventsMergedInMemory(
        userId: String,
        msgTypes: List<String>?,
        fromTs: Long?,
        toTs: Long?,
        status: String?,
        cursorTs: Long?,
        cursorLogId: String?,
        effectiveLimit: Int,
    ): CursorPage<AuditEvent> {
        val cap = minOf(10_000, maxOf(effectiveLimit * 4, effectiveLimit + 1, 256))
        val clientRows =
            queryClientReqLog(userId, msgTypes, fromTs, toTs, cursorTs, cursorLogId, cap)
        val messageRows =
            if (msgTypes == null || msgTypes.any { it in listOf("PUB", "EDIT", "DEL", "HDEL") }) {
                queryMessageLog(userId, null, msgTypes, fromTs, toTs, cursorTs, cursorLogId, cap)
            } else {
                emptyList()
            }

        val sorted =
            (clientRows + messageRows)
                .filter { ev -> status?.let { s -> ev.status == s } ?: true }
                .sortedWith(AuditEventSort.DESC)
                .distinctBy { it.eventId }

        val pageStart =
            if (cursorTs == null || cursorLogId == null) {
                0
            } else {
                val idx =
                    sorted.indexOfFirst { ev ->
                        ev.timestamp < cursorTs ||
                            (ev.timestamp == cursorTs && compareEventIdForSeek(ev.eventId, cursorLogId) < 0)
                    }
                if (idx == -1) sorted.size else idx
            }

        val window = sorted.drop(pageStart).take(effectiveLimit + 1)
        val hasMore = window.size > effectiveLimit
        val page = if (hasMore) window.dropLast(1) else window
        val nextCursor =
            if (hasMore && page.isNotEmpty()) {
                CursorCodec.encode(page.last().timestamp, page.last().eventId)
            } else {
                null
            }
        return CursorPage(data = page, nextCursor = nextCursor, hasMore = hasMore)
    }

    private fun compareEventIdForSeek(
        leftRaw: String,
        rightRaw: String,
    ): Int {
        val leftUuid = safeUuid(leftRaw)
        val rightUuid = safeUuid(rightRaw)

        return when {
            leftUuid != null && rightUuid != null -> compareUuidLsbFirst(leftUuid, rightUuid)
            else -> leftRaw.compareTo(rightRaw)
        }
    }

    private fun compareUuidLsbFirst(
        left: UUID,
        right: UUID,
    ): Int {
        val lsb = java.lang.Long.compare(left.leastSignificantBits, right.leastSignificantBits)
        if (lsb != 0) return lsb
        return java.lang.Long.compare(left.mostSignificantBits, right.mostSignificantBits)
    }

    private fun safeUuid(raw: String): UUID? = runCatching { UUID.fromString(raw) }.getOrNull()

    private fun queryClientReqLog(
        userId: String?,
        msgTypes: List<String>?,
        fromTs: Long?,
        toTs: Long?,
        cursorTs: Long?,
        cursorLogId: String?,
        limit: Int,
    ): List<AuditEvent> {
        val conditions =
            buildList {
                if (userId != null) add("sess_user_id = :userId")
                if (fromTs != null) add("log_timestamp >= fromUnixTimestamp64Milli(:fromTs)")
                if (toTs != null) add("log_timestamp <= fromUnixTimestamp64Milli(:toTs)")
                if (cursorTs != null && cursorLogId != null) {
                    add("(log_timestamp, log_id) < (fromUnixTimestamp64Milli(:cursorTs), toUUID(:cursorLogId))")
                }
                if (msgTypes != null) add("msg_type IN (:msgTypes)")
            }.let { if (it.isEmpty()) "1=1" else it.joinToString(" AND ") }

        val params =
            MapSqlParameterSource().apply {
                addValue("userId", userId)
                addValue("fromTs", fromTs)
                addValue("toTs", toTs)
                addValue("cursorTs", cursorTs)
                addValue("cursorLogId", cursorLogId)
                addValue("limit", limit)
                if (msgTypes != null) addValue("msgTypes", msgTypes)
            }

        return jdbc.query(
            """
            SELECT
                toString(log_id)                                           AS event_id,
                msg_type,
                toUnixTimestamp64Milli(log_timestamp)                      AS timestamp,
                sess_user_id                                               AS user_id,
                msg_topic                                                  AS topic_id,
                if(sess_auth_level != '' AND sess_auth_level != '0',
                   'success', 'failure')                                   AS status,
                sess_remote_addr                                           AS ip,
                sess_user_agent                                            AS user_agent,
                sess_device_id                                             AS device_id,
                sess_session_id,
                sub_topic,
                get_what,
                set_topic,
                del_what,
                del_user_id
            FROM audit.client_req_log
            WHERE $conditions
            ORDER BY log_timestamp DESC, log_id DESC
            LIMIT :limit
            """.trimIndent(),
            params,
        ) { rs, _ -> AuditEventMapping.mapAuditEventFromResultSet(rs) }
    }

    private fun queryMessageLog(
        userId: String?,
        topicId: String?,
        msgTypes: List<String>?,
        fromTs: Long?,
        toTs: Long?,
        cursorTs: Long?,
        cursorLogId: String?,
        limit: Int,
    ): List<AuditEvent> {
        val actionValues =
            msgTypes
                ?.mapNotNull {
                    when (it) {
                        "PUB" -> "CREATE"
                        "EDIT" -> "UPDATE"
                        "DEL", "HDEL" -> "DELETE"
                        else -> null
                    }
                }?.distinct()

        val conditions =
            buildList {
                if (userId != null) add("msg_from_user_id = :userId")
                if (topicId != null) add("msg_topic = :topicId")
                if (fromTs != null) add("log_timestamp >= fromUnixTimestamp64Milli(:fromTs)")
                if (toTs != null) add("log_timestamp <= fromUnixTimestamp64Milli(:toTs)")
                if (cursorTs != null && cursorLogId != null) {
                    add("(log_timestamp, log_id) < (fromUnixTimestamp64Milli(:cursorTs), toUUID(:cursorLogId))")
                }
                if (actionValues != null) add("toString(action) IN (:actionValues)")
            }.let { if (it.isEmpty()) "1=1" else it.joinToString(" AND ") }

        val params =
            MapSqlParameterSource().apply {
                addValue("userId", userId)
                addValue("topicId", topicId)
                addValue("fromTs", fromTs)
                addValue("toTs", toTs)
                addValue("cursorTs", cursorTs)
                addValue("cursorLogId", cursorLogId)
                addValue("limit", limit)
                if (actionValues != null) addValue("actionValues", actionValues)
            }

        return jdbc.query(
            """
            SELECT
                toString(log_id)                       AS event_id,
                CASE toString(action)
                    WHEN 'CREATE' THEN 'PUB'
                    WHEN 'UPDATE' THEN 'EDIT'
                    WHEN 'DELETE' THEN 'DEL'
                    ELSE toString(action)
                END                                    AS msg_type,
                toUnixTimestamp64Milli(log_timestamp)   AS timestamp,
                msg_from_user_id                       AS user_id,
                msg_topic                              AS topic_id,
                'success'                              AS status,
                ''                                     AS ip,
                ''                                     AS user_agent,
                ''                                     AS device_id,
                ''                                     AS sess_session_id,
                NULL                                   AS sub_topic,
                NULL                                   AS get_what,
                NULL                                   AS set_topic,
                NULL                                   AS del_what,
                NULL                                   AS del_user_id
            FROM audit.message_log
            WHERE $conditions
            ORDER BY log_timestamp DESC, log_id DESC
            LIMIT :limit
            """.trimIndent(),
            params,
        ) { rs, _ -> AuditEventMapping.mapAuditEventFromResultSet(rs) }
    }

    fun findEventById(eventId: String): AuditEvent? {
        val fromClient =
            runCatching {
                jdbc.query(
                    """
                    SELECT
                        toString(log_id)                                       AS event_id,
                        msg_type,
                        toUnixTimestamp64Milli(log_timestamp)                   AS timestamp,
                        sess_user_id                                           AS user_id,
                        msg_topic                                              AS topic_id,
                        if(sess_auth_level != '' AND sess_auth_level != '0',
                           'success', 'failure')                               AS status,
                        sess_remote_addr                                       AS ip,
                        sess_user_agent                                        AS user_agent,
                        sess_device_id                                         AS device_id,
                                sess_session_id,
                                sub_topic,
                                get_what,
                                set_topic,
                                del_what,
                                del_user_id
                    FROM audit.client_req_log
                    WHERE log_id = toUUID(:eventId)
                    LIMIT 1
                    """.trimIndent(),
                    MapSqlParameterSource("eventId", eventId),
                ) { rs, _ -> AuditEventMapping.mapAuditEventFromResultSet(rs) }
            }.getOrElse { emptyList() }

        if (fromClient.isNotEmpty()) return fromClient.first()

        return jdbc
            .query(
                """
                SELECT
                    toString(log_id)                       AS event_id,
                    CASE toString(action)
                        WHEN 'CREATE' THEN 'PUB'
                        WHEN 'UPDATE' THEN 'EDIT'
                        WHEN 'DELETE' THEN 'DEL'
                        ELSE toString(action)
                    END                                    AS msg_type,
                    toUnixTimestamp64Milli(log_timestamp)   AS timestamp,
                    msg_from_user_id                       AS user_id,
                    msg_topic                              AS topic_id,
                    'success'                              AS status,
                    ''                                     AS ip,
                    ''                                     AS user_agent,
                    ''                                     AS device_id,
                    ''                                     AS sess_session_id,
                    NULL                                   AS sub_topic,
                    NULL                                   AS get_what,
                    NULL                                   AS set_topic,
                    NULL                                   AS del_what,
                    NULL                                   AS del_user_id
                FROM audit.message_log
                WHERE toString(log_id) = :eventId
                LIMIT 1
                """.trimIndent(),
                MapSqlParameterSource("eventId", eventId),
            ) { rs, _ -> AuditEventMapping.mapAuditEventFromResultSet(rs) }
            .firstOrNull()
    }

    fun findAuthEvents(
        userId: String? = null,
        fromTs: Long? = null,
        toTs: Long? = null,
        cursor: String? = null,
        limit: Int = 100,
    ): CursorPage<AuditEvent> {
        val (cursorTs, cursorLogId) = decodeCursor(cursor)
        val effectiveLimit = limit.coerceIn(1, 1000)

        val conditions =
            buildList {
                add("msg_type IN ('LOGIN', 'HI', 'BYE', 'REG')")
                if (userId != null) add("sess_user_id = :userId")
                if (fromTs != null) add("log_timestamp >= fromUnixTimestamp64Milli(:fromTs)")
                if (toTs != null) add("log_timestamp <= fromUnixTimestamp64Milli(:toTs)")
                if (cursorTs != null && cursorLogId != null) {
                    add("(log_timestamp, log_id) < (fromUnixTimestamp64Milli(:cursorTs), toUUID(:cursorLogId))")
                }
            }

        val params =
            MapSqlParameterSource().apply {
                addValue("userId", userId)
                addValue("fromTs", fromTs)
                addValue("toTs", toTs)
                addValue("cursorTs", cursorTs)
                addValue("cursorLogId", cursorLogId)
                addValue("limit", effectiveLimit + 1)
            }

        val rows =
            jdbc.query(
                """
                SELECT
                    toString(log_id)                                       AS event_id,
                    msg_type,
                    toUnixTimestamp64Milli(log_timestamp)                   AS timestamp,
                    sess_user_id                                           AS user_id,
                    msg_topic                                              AS topic_id,
                    if(sess_auth_level != '' AND sess_auth_level != '0',
                       'success', 'failure')                               AS status,
                    sess_remote_addr                                       AS ip,
                    sess_user_agent                                        AS user_agent,
                    sess_device_id                                         AS device_id,
                          sess_session_id,
                          sub_topic,
                          get_what,
                          set_topic,
                          del_what,
                          del_user_id
                FROM audit.client_req_log
                WHERE ${conditions.joinToString(" AND ")}
                ORDER BY log_timestamp DESC, log_id DESC
                LIMIT :limit
                """.trimIndent(),
                params,
            ) { rs, _ -> AuditEventMapping.mapAuditEventFromResultSet(rs) }

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
}
