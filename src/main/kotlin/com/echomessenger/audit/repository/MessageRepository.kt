package com.echomessenger.audit.repository

import com.echomessenger.audit.domain.MessageReportItem
import com.echomessenger.audit.domain.MessageReportRequest
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository

@Repository
class MessageRepository(
    private val jdbc: NamedParameterJdbcTemplate,
) {
    fun countMessages(req: MessageReportRequest): Long {
        val (sql, params) = buildQuery(req, countOnly = true)
        return jdbc.queryForObject(sql, params, Long::class.java) ?: 0L
    }

    fun findMessages(
        req: MessageReportRequest,
        limit: Int = 10_000,
    ): List<MessageReportItem> {
        val (sql, params) = buildQuery(req, countOnly = false, limit = limit)
        return jdbc.query(sql, params) { rs, _ ->
            MessageReportItem(
                messageId = rs.getLong("seq_id"),
                topicId = rs.getString("topic_id"),
                userId = rs.getString("usr_id"),
                userName = rs.getString("user_name")?.takeIf { it.isNotBlank() },
                timestamp = rs.getLong("timestamp"),
                content = rs.getString("content")?.takeIf { it.isNotBlank() },
                isDeleted = rs.getBoolean("is_deleted"),
            )
        }
    }

    fun streamMessages(
        req: MessageReportRequest,
        batchSize: Int = 1000,
        consumer: (List<MessageReportItem>) -> Unit,
    ) {
        // Стриминг через batched cursor — не загружаем всё в память
        var offset = 0L
        while (true) {
            val (sql, params) = buildQuery(req, countOnly = false, limit = batchSize, offset = offset)
            val batch =
                jdbc.query(sql, params) { rs, _ ->
                    MessageReportItem(
                        messageId = rs.getLong("seq_id"),
                        topicId = rs.getString("topic_id"),
                        userId = rs.getString("usr_id"),
                        userName = rs.getString("user_name")?.takeIf { it.isNotBlank() },
                        timestamp = rs.getLong("timestamp"),
                        content = rs.getString("content")?.takeIf { it.isNotBlank() },
                        isDeleted = rs.getBoolean("is_deleted"),
                    )
                }
            if (batch.isEmpty()) break
            consumer(batch)
            if (batch.size < batchSize) break
            offset += batchSize
        }
    }

    private fun buildQuery(
        req: MessageReportRequest,
        countOnly: Boolean,
        limit: Int = 10_000,
        offset: Long = 0,
    ): Pair<String, MapSqlParameterSource> {
        val conditions =
            buildList {
                add("msg_ts >= fromUnixTimestamp64Milli(:fromTs)")
                add("msg_ts <= fromUnixTimestamp64Milli(:toTs)")
                if (!req.users.isNullOrEmpty()) add("usr_id IN (:users)")
                if (!req.topics.isNullOrEmpty()) add("topic_id IN (:topics)")
                if (!req.includeDeleted) add("msg_type != 'HDEL'")
            }

        val params =
            MapSqlParameterSource().apply {
                addValue("fromTs", req.fromTs)
                addValue("toTs", req.toTs)
                if (!req.users.isNullOrEmpty()) addValue("users", req.users)
                if (!req.topics.isNullOrEmpty()) addValue("topics", req.topics)
                addValue("limit", limit)
                addValue("offset", offset)
            }

        val selectClause =
            if (countOnly) {
                "SELECT count() AS cnt"
            } else {
                """
                SELECT
                    m.seq_id,
                    m.topic_id,
                    m.usr_id,
                    a.display_name AS user_name,
                    toUnixTimestamp64Milli(m.msg_ts) AS timestamp,
                    m.content,
                    (m.msg_type = 'HDEL') AS is_deleted
                """.trimIndent()
            }

        val sql =
            if (countOnly) {
                """
                $selectClause
                FROM audit.message_log m
                WHERE ${conditions.joinToString(" AND ")}
                """.trimIndent()
            } else {
                """
                $selectClause
                FROM audit.message_log m
                LEFT JOIN audit.account_log a ON m.usr_id = a.user_id
                WHERE ${conditions.joinToString(" AND ")}
                ORDER BY m.msg_ts DESC, m.seq_id DESC
                LIMIT :limit OFFSET :offset
                """.trimIndent()
            }

        return sql to params
    }
}
