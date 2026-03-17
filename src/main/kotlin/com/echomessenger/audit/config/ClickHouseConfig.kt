package com.echomessenger.audit.config

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import javax.sql.DataSource

@Configuration
class ClickHouseConfig(
    @Value("\${clickhouse.url}") private val url: String,
    @Value("\${clickhouse.user}") private val user: String,
    @Value("\${clickhouse.password}") private val password: String,
    @Value("\${clickhouse.pool.maximum-pool-size:10}") private val maxPoolSize: Int,
    @Value("\${clickhouse.pool.minimum-idle:2}") private val minIdle: Int,
    @Value("\${clickhouse.pool.connection-timeout:5000}") private val connectionTimeout: Long,
) {
    @Bean
    fun clickHouseDataSource(): DataSource {
        // jdbcUrl-режим HikariCP — credentials вшиты в URL через параметры ClickHouse JDBC.
        // Не используем dataSourceClassName т.к. он требует отдельного маппинга properties
        // и конфликтует с тем как clickhouse-jdbc 0.7.x ожидает user/password.
        // session_readonly убран — невалидный параметр в 0.7.x, ломает handshake.
        val jdbcUrl = buildUrl(url, user, password)

        val hikariConfig =
            HikariConfig().apply {
                this.jdbcUrl = jdbcUrl
                this.maximumPoolSize = maxPoolSize
                this.minimumIdle = minIdle
                this.connectionTimeout = connectionTimeout
                this.idleTimeout = 600_000L
                this.maxLifetime = 1_800_000L
                this.poolName = "audit-clickhouse-pool"
                // ClickHouse не поддерживает стандартные JDBC isValid() — используем лёгкий запрос
                this.connectionTestQuery = "SELECT 1"
            }

        return HikariDataSource(hikariConfig)
    }

    @Bean
    fun jdbcTemplate(dataSource: DataSource) = NamedParameterJdbcTemplate(dataSource)

    private fun buildUrl(
        baseUrl: String,
        user: String,
        password: String,
    ): String {
        // Если URL уже содержит параметры — добавляем через &, иначе через ?
        val separator = if (baseUrl.contains('?')) '&' else '?'
        return "${baseUrl}${separator}user=$user&password=$password"
    }
}
