package com.echomessenger.audit.repository

import com.echomessenger.audit.domain.MessageReportItem
import com.echomessenger.audit.domain.MessageReportRequest
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository

@Repository
class MessageRepository(
    @Qualifier("clickHouseJdbcTemplate")
    private val jdbc: NamedParameterJdbcTemplate,
) {
    private val log = LoggerFactory.getLogger(MessageRepository::class.java)

    fun countMessages(req: MessageReportRequest): Long {
        val (whereClause, params) = buildBaseWhere(req)
        val sql =
            """
            SELECT count() AS cnt
            FROM audit.message_log m
            WHERE $whereClause
            """.trimIndent()
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
            val (sql, params) = buildStreamBatchQuery(
                req = req,
                limit = batchSize,
                lastTimestamp = lastTimestamp,
                lastSeqId = lastSeqId
            )

            val page = jdbc.query(sql, params) { rs, _ ->
                MessageReportItem(
                    messageId = rs.getLong("msg_seq_id"),
                    topicId = rs.getString("msg_topic"),
                    userId = rs.getString("msg_from_user_id"),
                    userName = null,
                    timestamp = rs.getLong("timestamp"),
                    content = rs.getString("msg_content")?.takeIf { it.isNotBlank() },
                    isDeleted = rs.getBoolean("is_deleted")
                )
            }

            if (page.isEmpty()) break

            val publicByUserId = loadPublicNamesForUserIds(page.mapNotNull { it.userId })
            val batch =
                page.map { item ->
                    val userName = item.userName ?: publicByUserId[item.userId]
                    if (userName.isNullOrBlank()) item else item.copy(userName = userName)
                }

            val repositoryResolved = batch.count { !it.userName.isNullOrBlank() }
            val unresolved = batch.size - repositoryResolved
            log.info(
                "streamMessages batch loaded size={} distinctUsers={} repositoryResolved={} unresolved={} lastTimestamp={} lastSeqId={}",
                batch.size,
                page.mapNotNull { it.userId }.toSet().size,
                repositoryResolved,
                unresolved,
                lastTimestamp,
                lastSeqId,
            )

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

    private fun buildBaseWhere(req: MessageReportRequest): Pair<String, MapSqlParameterSource> {
        val conditions = buildList {
            add("m.log_timestamp >= fromUnixTimestamp64Milli(:fromTs)")
            add("m.log_timestamp <= fromUnixTimestamp64Milli(:toTs)")
            if (!req.users.isNullOrEmpty()) add("m.msg_from_user_id IN (:users)")
            if (!req.topics.isNullOrEmpty()) add("m.msg_topic IN (:topics)")
            if (!req.includeDeleted) add("toString(m.action) != 'DELETE'")
        }

        val params = MapSqlParameterSource().apply {
            addValue("fromTs", req.fromTs)
            addValue("toTs", req.toTs)
            if (!req.users.isNullOrEmpty()) addValue("users", req.users)
            if (!req.topics.isNullOrEmpty()) addValue("topics", req.topics)
        }

        return conditions.joinToString(" AND ") to params
    }

    private fun buildStreamBatchQuery(
        req: MessageReportRequest,
        limit: Int,
        lastTimestamp: Long? = null,
        lastSeqId: Long? = null,
    ): Pair<String, MapSqlParameterSource> {
        val (baseWhereClause, baseParams) = buildBaseWhere(req)
        val conditions = mutableListOf(baseWhereClause)
        val params = MapSqlParameterSource(baseParams.values)

        if (lastTimestamp != null && lastSeqId != null) {
            conditions.add("(m.log_timestamp, m.msg_seq_id) < (fromUnixTimestamp64Milli(:lastTs), :lastSeqId)")
            params.addValue("lastTs", lastTimestamp)
            params.addValue("lastSeqId", lastSeqId)
        }
        params.addValue("limit", limit)

        val sql =
            """
            SELECT
                m.msg_seq_id,
                m.msg_topic,
                m.msg_from_user_id,
                toUnixTimestamp64Milli(m.log_timestamp) AS timestamp,
                m.msg_content,
                (toString(m.action) = 'DELETE') AS is_deleted
            FROM audit.message_log m
            WHERE ${conditions.joinToString(" AND ")}
            ORDER BY m.log_timestamp DESC, m.msg_seq_id DESC
            LIMIT :limit
            """.trimIndent()

        return sql to params
    }

    private fun loadPublicNamesForUserIds(userIds: List<String>): Map<String, String> {
        val distinctUserIds = userIds.map { it.trim() }.filter { it.isNotEmpty() }.distinct()
        if (distinctUserIds.isEmpty()) return emptyMap()

        return jdbc.query(
            """
            SELECT user_id, argMax(public, log_timestamp) AS public
            FROM audit.account_log
            WHERE user_id IN (:userIds)
            GROUP BY user_id
            """.trimIndent(),
            MapSqlParameterSource("userIds", distinctUserIds),
        ) { rs, _ ->
            val userId = rs.getString("user_id")
            val public = rs.getString("public")?.takeIf { it.isNotBlank() }
            userId to public
        }.mapNotNull { (userId, public) -> public?.let { userId to it } }.toMap()
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