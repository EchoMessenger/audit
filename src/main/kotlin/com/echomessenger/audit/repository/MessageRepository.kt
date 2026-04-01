package com.echomessenger.audit.repository

import com.echomessenger.audit.domain.MessageReportItem
import com.echomessenger.audit.domain.MessageReportRequest
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository

@Repository
class MessageRepository(
    @Qualifier("clickHouseJdbcTemplate")
    private val jdbc: NamedParameterJdbcTemplate,
) {
    fun countMessages(req: MessageReportRequest): Long {
        val (sql, params) = buildQuery(req, countOnly = true)
        return jdbc.queryForObject(sql, params, Long::class.java) ?: 0L
    }

    fun findMessages(
        req: MessageReportRequest,
        limit: Int = 10_000,
        startSeqId: Long? = null,
    ): List<MessageReportItem> {
        val (sql, params) = buildQuery(req, countOnly = false, limit = limit, lastSeqId = startSeqId)
        return jdbc.query(sql, params) { rs, _ ->
            MessageReportItem(
                messageId = rs.getLong("msg_seq_id"),
                topicId = rs.getString("msg_topic"),
                userId = rs.getString("msg_from_user_id"),
                userName = rs.getString("user_name")?.takeIf { it.isNotBlank() },
                timestamp = rs.getLong("timestamp"),
                content = rs.getString("msg_content")?.takeIf { it.isNotBlank() },
                isDeleted = rs.getBoolean("is_deleted"),
            )
        }
    }

    fun streamMessages(
        req: MessageReportRequest,
        batchSize: Int = 1000,
        consumer: (List<MessageReportItem>) -> Unit,
    ) {
        var lastTimestamp: Long? = null
        var lastSeqId: Long? = null
        val seen = mutableSetOf<Long>()

        while (true) {
            val (sql, params) = buildQuery(
                req = req,
                countOnly = false,
                limit = batchSize,
                lastTimestamp = lastTimestamp,
                lastSeqId = lastSeqId
            )

            val batch = jdbc.query(sql, params) { rs, _ ->
                MessageReportItem(
                    messageId = rs.getLong("msg_seq_id"),
                    topicId = rs.getString("msg_topic"),
                    userId = rs.getString("msg_from_user_id"),
                    userName = rs.getString("user_name")?.takeIf { it.isNotBlank() },
                    timestamp = rs.getLong("timestamp"),
                    content = rs.getString("msg_content")?.takeIf { it.isNotBlank() },
                    isDeleted = rs.getBoolean("is_deleted")
                )
            }

            if (batch.isEmpty()) break

            // Отфильтровываем дубликаты (на всякий случай)
            val uniqueBatch = batch.filter { seen.add(it.messageId) }
            if (uniqueBatch.isNotEmpty()) {
                consumer(uniqueBatch)
            }

            // Берем последний элемент батча как cursor
            val last = batch.last()
            lastTimestamp = last.timestamp
            lastSeqId = last.messageId

            if (batch.size < batchSize) break
        }
    }

    private fun buildQuery(
        req: MessageReportRequest,
        countOnly: Boolean,
        limit: Int = 1000,
        lastTimestamp: Long? = null,
        lastSeqId: Long? = null
    ): Pair<String, MapSqlParameterSource> {

        val conditions = buildList {
            add("m.log_timestamp >= fromUnixTimestamp64Milli(:fromTs)")
            add("m.log_timestamp <= fromUnixTimestamp64Milli(:toTs)")
            if (!req.users.isNullOrEmpty()) add("m.msg_from_user_id IN (:users)")
            if (!req.topics.isNullOrEmpty()) add("m.msg_topic IN (:topics)")
            if (!req.includeDeleted) add("toString(m.action) != 'DELETE'")

            // cursor фильтр для постраничной выборки
            if (lastTimestamp != null && lastSeqId != null) {
                add("(m.log_timestamp, m.msg_seq_id) < (fromUnixTimestamp64Milli(:lastTs), :lastSeqId)")
            }
        }

        val params = MapSqlParameterSource().apply {
            addValue("fromTs", req.fromTs)
            addValue("toTs", req.toTs)
            if (!req.users.isNullOrEmpty()) addValue("users", req.users)
            if (!req.topics.isNullOrEmpty()) addValue("topics", req.topics)
            addValue("limit", limit)
            if (lastTimestamp != null && lastSeqId != null) {
                addValue("lastTs", lastTimestamp)
                addValue("lastSeqId", lastSeqId)
            }
        }

        val sql = if (countOnly) {
            """
        SELECT count() AS cnt
        FROM audit.message_log m
        LEFT JOIN (
            SELECT user_id, argMax(public, log_timestamp) AS public
            FROM audit.account_log
            GROUP BY user_id
        ) AS a ON m.msg_from_user_id = a.user_id
        WHERE ${conditions.joinToString(" AND ")}
        """.trimIndent()
        } else {
            """
        SELECT
            m.msg_seq_id,
            m.msg_topic,
            m.msg_from_user_id,
            a.public AS user_name,
            toUnixTimestamp64Milli(m.log_timestamp) AS timestamp,
            m.msg_content,
            (toString(m.action) = 'DELETE') AS is_deleted
        FROM audit.message_log m
        LEFT JOIN (
            SELECT user_id, argMax(public, log_timestamp) AS public
            FROM audit.account_log
            GROUP BY user_id
        ) AS a ON m.msg_from_user_id = a.user_id
        WHERE ${conditions.joinToString(" AND ")}
        ORDER BY m.log_timestamp DESC, m.msg_seq_id DESC
        LIMIT :limit
        """.trimIndent()
        }

        return sql to params
    }
}