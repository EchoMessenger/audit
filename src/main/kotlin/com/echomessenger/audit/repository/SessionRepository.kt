package com.echomessenger.audit.repository

import com.echomessenger.audit.domain.CursorCodec
import com.echomessenger.audit.domain.CursorPage
import com.echomessenger.audit.domain.SessionSummary
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository

@Repository
class SessionRepository(
    private val jdbc: NamedParameterJdbcTemplate,
) {
    /**
     * GET /audit/sessions — группировка по sess_session_id из client_req_log.
     * Возвращает агрегированную статистику по каждой сессии.
     */
    fun findSessions(
        userId: String? = null,
        fromTs: Long? = null,
        toTs: Long? = null,
        cursor: String? = null,
        limit: Int = 50,
    ): CursorPage<SessionSummary> {
        val effectiveLimit = limit.coerceIn(1, 500)
        val (cursorTs, _) = if (cursor != null) CursorCodec.decode(cursor) else null to null

        val conditions =
            buildList {
                add("sess_session_id != ''")
                if (userId != null) add("sess_user_id = :userId")
                if (fromTs != null) add("req_ts >= fromUnixTimestamp64Milli(:fromTs)")
                if (toTs != null) add("req_ts <= fromUnixTimestamp64Milli(:toTs)")
                if (cursorTs != null) add("max_ts < fromUnixTimestamp64Milli(:cursorTs)")
            }

        val params =
            MapSqlParameterSource().apply {
                addValue("userId", userId)
                addValue("fromTs", fromTs)
                addValue("toTs", toTs)
                addValue("cursorTs", cursorTs)
                addValue("limit", effectiveLimit + 1)
            }

        // HAVING на агрегатах — фильтр cursor применяется через подзапрос
        val sql =
            """
            SELECT
                sess_session_id                                     AS session_id,
                any(sess_user_id)                                   AS user_id,
                toUnixTimestamp64Milli(min(req_ts))                 AS first_event_at,
                toUnixTimestamp64Milli(max(req_ts))                 AS last_event_at,
                toInt64(dateDiff('second', min(req_ts), max(req_ts))) AS duration_seconds,
                count()                                             AS event_count,
                groupUniqArray(client_ip)                           AS ip_addresses
            FROM audit.client_req_log
            WHERE ${conditions.filter { !it.contains("max_ts") }.joinToString(" AND ")}
            GROUP BY sess_session_id
            ${if (cursorTs != null) "HAVING toUnixTimestamp64Milli(max(req_ts)) < :cursorTs" else ""}
            ORDER BY last_event_at DESC
            LIMIT :limit
            """.trimIndent()

        val rows =
            jdbc.query(sql, params) { rs, _ ->
                @Suppress("UNCHECKED_CAST")
                SessionSummary(
                    sessionId = rs.getString("session_id"),
                    userId = rs.getString("user_id")?.takeIf { it.isNotBlank() },
                    firstEventAt = rs.getLong("first_event_at"),
                    lastEventAt = rs.getLong("last_event_at"),
                    durationSeconds = rs.getLong("duration_seconds"),
                    eventCount = rs.getLong("event_count"),
                    ipAddresses =
                        (rs.getArray("ip_addresses")?.array as? Array<*>)
                            ?.filterIsInstance<String>() ?: emptyList(),
                )
            }

        val hasMore = rows.size > effectiveLimit
        val page = if (hasMore) rows.dropLast(1) else rows
        val nextCursor =
            if (hasMore && page.isNotEmpty()) {
                CursorCodec.encode(page.last().lastEventAt, page.last().sessionId)
            } else {
                null
            }

        return CursorPage(data = page, nextCursor = nextCursor, hasMore = hasMore)
    }
}
