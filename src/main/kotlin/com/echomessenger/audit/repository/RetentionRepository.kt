package com.echomessenger.audit.repository

import com.echomessenger.audit.domain.RetentionPolicy
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Repository
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

@Repository
class RetentionRepository(
    @Value("\${clickhouse.url}") private val jdbcUrl: String,
    @Value("\${clickhouse.user}") private val user: String,
    @Value("\${clickhouse.password}") private val password: String,
) {
    private val httpClient = HttpClient.newHttpClient()
    private val mapper = jacksonObjectMapper()

    /**
     * Читает метаданные таблиц из system.tables через HTTP API.
     * TTL парсится из create_table_query (DDL) — колонка ttl_expression
     * появилась только в ClickHouse 24.8+, в 24.3 её нет.
     */
    fun findRetentionPolicies(): List<RetentionPolicy> {
        val baseUrl = buildHttpBaseUrl()

        val sql =
            "SELECT name, database, engine, create_table_query " +
                "FROM system.tables WHERE database = 'audit' ORDER BY name FORMAT JSONEachRow"

        val response = execHttp(baseUrl, sql)

        return response
            .lines()
            .filter { it.isNotBlank() }
            .map { line ->
                val row = mapper.readValue<Map<String, String>>(line)
                val ddl = row["create_table_query"] ?: ""
                val ttlExpr = extractTtlFromDdl(ddl)
                RetentionPolicy(
                    tableName = row["name"] ?: "",
                    database = row["database"] ?: "",
                    engine = row["engine"] ?: "",
                    ttlExpression = ttlExpr,
                    retentionDays = parseTtlDays(ttlExpr),
                )
            }
    }

    private fun buildHttpBaseUrl(): String =
        jdbcUrl
            .removePrefix("jdbc:")
            .substringBefore("?")
            .substringBefore("/audit")
            .replace("clickhouse://", "http://")

    private fun execHttp(
        baseUrl: String,
        sql: String,
    ): String {
        val uri = URI.create("$baseUrl/?user=$user&password=$password")
        val request =
            HttpRequest
                .newBuilder()
                .uri(uri)
                .POST(HttpRequest.BodyPublishers.ofString(sql))
                .build()
        val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
        if (response.statusCode() != 200) {
            throw RuntimeException("system.tables query failed: ${response.body()}")
        }
        return response.body()
    }

    /**
     * Извлекаем TTL из DDL строки CREATE TABLE.
     * Пример: "...TTL req_ts + toIntervalDay(90)..."
     */
    private fun extractTtlFromDdl(ddl: String): String? {
        val ttlRegex = Regex("""TTL\s+(.+?)(?:\s+SETTINGS|\s+ORDER BY|\s*$)""", RegexOption.IGNORE_CASE)
        return ttlRegex
            .find(ddl)
            ?.groupValues
            ?.get(1)
            ?.trim()
            ?.takeIf { it.isNotBlank() }
    }

    private fun parseTtlDays(expr: String?): Int? {
        if (expr == null) return null
        return Regex("""toIntervalDay\((\d+)\)""")
            .find(expr)
            ?.groupValues
            ?.get(1)
            ?.toIntOrNull()
    }
}
