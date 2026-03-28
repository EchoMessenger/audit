package com.echomessenger.audit.config

import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import javax.sql.DataSource

@Configuration
class ClickHouseConfig(
    @Value("\${clickhouse.http-url}") private val httpUrl: String,
    @Value("\${clickhouse.username}") private val username: String,
    @Value("\${clickhouse.password}") private val password: String,
    @Value("\${clickhouse.database:audit}") private val database: String,
) {
    @Bean
    fun clickHouseDataSource(): DataSource =
        DriverManagerDataSource().apply {
            // Credentials в URL — clickhouse-jdbc 0.7.x не берёт их из setUsername/setPassword
            url = buildJdbcUrl()
        }

    @Bean
    fun clickHouseJdbcTemplate(
        @Qualifier("clickHouseDataSource") dataSource: DataSource,
    ) = NamedParameterJdbcTemplate(dataSource)

    private fun buildJdbcUrl(): String =
        httpUrl
            .removePrefix("http://")
            .let { "jdbc:clickhouse://$it/$database?user=$username&password=$password" }
}