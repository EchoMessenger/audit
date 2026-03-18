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
    @Value("\${clickhouse.http-url}") private val httpUrl: String,
    @Value("\${clickhouse.username}") private val username: String,
    @Value("\${clickhouse.password}") private val password: String,
) {
    private val httpClient = HttpClient.newHttpClient()
    private val mapper = jacksonObjectMapper()

    fun findRetentionPolicies(): List<RetentionPolicy> {
        val sql =
            "SELECT name, database, engine, create_table_query " +
                "FROM system.tables WHERE database = 'audit' ORDER BY name FORMAT JSONEachRow"

        return execHttp(sql)
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

    private fun execHttp(sql: String): String {
        val request = HttpRequest.newBuilder()
            .uri(URI.create("$httpUrl/"))
            // Заголовки безопаснее query params — не логируются Traefik/nginx
            .header("X-ClickHouse-User", username)
            .header("X-ClickHouse-Key", password)
            .POST(HttpRequest.BodyPublishers.ofString(sql))
            .build()

        val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
        if (response.statusCode() != 200) {
            throw RuntimeException("ClickHouse HTTP query failed [${response.statusCode()}]: ${response.body()}")
        }
        return response.body()
    }

    private fun extractTtlFromDdl(ddl: String): String? =
        Regex("""TTL\s+(.+?)(?:\s+SETTINGS|\s+ORDER BY|\s*$)""", RegexOption.IGNORE_CASE)
            .find(ddl)?.groupValues?.get(1)?.trim()?.takeIf { it.isNotBlank() }

    private fun parseTtlDays(expr: String?): Int? =
        expr?.let {
            Regex("""toIntervalDay\((\d+)\)""").find(it)?.groupValues?.get(1)?.toIntOrNull()
        }
}
