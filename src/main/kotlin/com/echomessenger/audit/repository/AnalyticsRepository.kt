package com.echomessenger.audit.repository

import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.cache.annotation.Cacheable
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository

data class SummaryResult(
    val eventsByType: Map<String, Long>,
    val topUsers: List<UserActivity>,
    val byHour: List<HourlyBucket>,
)

data class UserActivity(val userId: String, val count: Long)
data class HourlyBucket(val hour: Int, val count: Long)
data class TimeseriesPoint(val ts: Long, val value: Long)

@Repository
class AnalyticsRepository(
    @Qualifier("clickHouseJdbcTemplate")
    private val jdbc: NamedParameterJdbcTemplate,
) {
    @Cacheable("analytics-summary")
    fun getSummary(fromTs: Long, toTs: Long): SummaryResult {
        val params = MapSqlParameterSource()
            .addValue("fromTs", fromTs)
            .addValue("toTs", toTs)

        val eventsByType = jdbc.query(
            """
            SELECT msg_type, sum(cnt) AS total
            FROM audit.mv_daily_msg_type_stats
            WHERE day >= toDate(fromUnixTimestamp64Milli(:fromTs))
              AND day <= toDate(fromUnixTimestamp64Milli(:toTs))
            GROUP BY msg_type
            """.trimIndent(), params,
        ) { rs, _ -> rs.getString("msg_type") to rs.getLong("total") }
            .associate { (type, count) -> AuditRepository.mapMsgTypeToEventType(type) to count }

        val topUsers = jdbc.query(
            """
            SELECT usr_id, sum(event_count) AS total
            FROM audit.mv_daily_user_activity
            WHERE day >= toDate(fromUnixTimestamp64Milli(:fromTs))
              AND day <= toDate(fromUnixTimestamp64Milli(:toTs))
            GROUP BY usr_id
            ORDER BY total DESC
            LIMIT 20
            """.trimIndent(), params,
        ) { rs, _ -> UserActivity(userId = rs.getString("usr_id"), count = rs.getLong("total")) }

        val byHour = jdbc.query(
            """
            SELECT toHour(hour_ts) AS hour, sum(event_count) AS cnt
            FROM audit.mv_hourly_load_stats
            WHERE hour_ts >= toDateTime(fromUnixTimestamp64Milli(:fromTs))
              AND hour_ts <= toDateTime(fromUnixTimestamp64Milli(:toTs))
            GROUP BY hour
            ORDER BY hour
            """.trimIndent(), params,
        ) { rs, _ -> HourlyBucket(hour = rs.getInt("hour"), count = rs.getLong("cnt")) }

        return SummaryResult(eventsByType = eventsByType, topUsers = topUsers, byHour = byHour)
    }

    @Cacheable("analytics-timeseries")
    fun getTimeseries(metric: String, interval: String, fromTs: Long, toTs: Long): List<TimeseriesPoint> {
        val msgType = mapMetricToMsgType(metric)
        val params = MapSqlParameterSource()
            .addValue("fromTs", fromTs)
            .addValue("toTs", toTs)
            .addValue("msgType", msgType)

        return when (interval.lowercase()) {
            "hour" -> jdbc.query(
                """
                SELECT
                    toUnixTimestamp64Milli(toDateTime64(hour_ts, 3)) AS ts,
                    sum(event_count) AS value
                FROM audit.mv_hourly_load_stats
                WHERE hour_ts >= toDateTime(fromUnixTimestamp64Milli(:fromTs))
                  AND hour_ts <= toDateTime(fromUnixTimestamp64Milli(:toTs))
                  AND msg_type = :msgType
                GROUP BY hour_ts
                ORDER BY hour_ts
                """.trimIndent(), params,
            ) { rs, _ -> TimeseriesPoint(ts = rs.getLong("ts"), value = rs.getLong("value")) }

            "day" -> jdbc.query(
                """
                SELECT
                    toUnixTimestamp64Milli(toDateTime64(day, 3)) AS ts,
                    sum(cnt) AS value
                FROM audit.mv_daily_msg_type_stats
                WHERE day >= toDate(fromUnixTimestamp64Milli(:fromTs))
                  AND day <= toDate(fromUnixTimestamp64Milli(:toTs))
                  AND msg_type = :msgType
                GROUP BY day
                ORDER BY day
                """.trimIndent(), params,
            ) { rs, _ -> TimeseriesPoint(ts = rs.getLong("ts"), value = rs.getLong("value")) }

            else -> throw IllegalArgumentException("Unsupported interval: $interval. Use 'hour' or 'day'")
        }
    }

    private fun mapMetricToMsgType(metric: String): String = when (metric) {
        "message.create" -> "PUB"
        "message.delete" -> "DEL"
        "auth.login" -> "LOGIN"
        "auth.register" -> "REG"
        "topic.create" -> "CREATE"
        else -> metric.substringAfterLast(".").uppercase()
    }
}