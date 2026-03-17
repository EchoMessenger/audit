package com.echomessenger.audit.integration

import com.echomessenger.audit.config.TestSecurityConfig
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestSecurityConfig::class)
abstract class IntegrationTestBase {
    companion object {
        private const val CLICKHOUSE_HTTP_PORT = 8123
        private const val CH_USER = "default"
        private const val CH_PASSWORD = "test_secret"

        private val clickHouse: GenericContainer<*> =
            GenericContainer(
                DockerImageName.parse("clickhouse/clickhouse-server:24.3-alpine"),
            ).withExposedPorts(CLICKHOUSE_HTTP_PORT)
                // ClickHouse 24.x требует явных credentials — без них генерируется рандомный пароль
                .withEnv("CLICKHOUSE_USER", CH_USER)
                .withEnv("CLICKHOUSE_PASSWORD", CH_PASSWORD)
                .withEnv("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")
                .waitingFor(
                    Wait
                        .forHttp("/ping")
                        .forPort(CLICKHOUSE_HTTP_PORT)
                        .forStatusCode(200)
                        .withStartupTimeout(Duration.ofSeconds(120)),
                )

        init {
            clickHouse.start()
            initSchema()
        }

        private fun execHttp(
            baseUrl: String,
            sql: String,
        ) {
            val client = HttpClient.newHttpClient()
            // Credentials передаём через query params — стандартный HTTP API ClickHouse
            val uri = URI.create("$baseUrl/?user=$CH_USER&password=$CH_PASSWORD")
            val request =
                HttpRequest
                    .newBuilder()
                    .uri(uri)
                    .POST(HttpRequest.BodyPublishers.ofString(sql))
                    .build()
            val response = client.send(request, HttpResponse.BodyHandlers.ofString())
            if (response.statusCode() != 200) {
                throw RuntimeException("ClickHouse DDL failed.\nSQL: $sql\nResponse: ${response.body()}")
            }
        }

        private fun initSchema() {
            val baseUrl = "http://${clickHouse.host}:${clickHouse.getMappedPort(CLICKHOUSE_HTTP_PORT)}"

            val statements =
                listOf(
                    "CREATE DATABASE IF NOT EXISTS audit",
                    // ── Основные таблицы ──────────────────────────────────────────
                    """CREATE TABLE IF NOT EXISTS audit.client_req_log (
                    log_id          UUID              DEFAULT generateUUIDv4(),
                    req_ts          DateTime64(3)     DEFAULT now64(3),
                    msg_type        LowCardinality(String),
                    sess_user_id    String            DEFAULT '',
                    sess_auth_level UInt8             DEFAULT 0,
                    sess_session_id String            DEFAULT '',
                    sess_device_id  String            DEFAULT '',
                    client_ip       String            DEFAULT '',
                    user_agent      String            DEFAULT '',
                    device_id       String            DEFAULT ''
                ) ENGINE = MergeTree() ORDER BY (req_ts, log_id)""",
                    """CREATE TABLE IF NOT EXISTS audit.message_log (
                    seq_id    UInt64,
                    msg_ts    DateTime64(3) DEFAULT now64(3),
                    msg_type  LowCardinality(String),
                    usr_id    String        DEFAULT '',
                    topic_id  String        DEFAULT '',
                    content   String        DEFAULT ''
                ) ENGINE = MergeTree() ORDER BY (msg_ts, seq_id)""",
                    """CREATE TABLE IF NOT EXISTS audit.account_log (
                    user_id      String,
                    display_name String        DEFAULT '',
                    updated_at   DateTime64(3) DEFAULT now64(3)
                ) ENGINE = ReplacingMergeTree(updated_at) ORDER BY (user_id)""",
                    """CREATE TABLE IF NOT EXISTS audit.subscription_log (
                    event_ts DateTime64(3) DEFAULT now64(3),
                    user_id  String        DEFAULT '',
                    topic_id String        DEFAULT '',
                    action   String        DEFAULT '',
                    new_role String        DEFAULT '',
                    old_role String        DEFAULT ''
                ) ENGINE = MergeTree() ORDER BY (event_ts, user_id)""",
                    // ── incident_log ───────────────────────────────────────────────
                    """CREATE TABLE IF NOT EXISTS audit.incident_log (
                    incident_id UUID          DEFAULT generateUUIDv4(),
                    detected_at DateTime64(3) DEFAULT now64(3),
                    type        String        DEFAULT '',
                    status      String        DEFAULT 'open',
                    user_id     String        DEFAULT '',
                    details     String        DEFAULT '{}',
                    updated_at  DateTime64(3) DEFAULT now64(3)
                ) ENGINE = ReplacingMergeTree(updated_at)
                  PARTITION BY toYYYYMM(detected_at)
                  ORDER BY (incident_id)""",
                    // ── export_job_log ─────────────────────────────────────────────
                    """CREATE TABLE IF NOT EXISTS audit.export_job_log (
                    export_id       UUID          DEFAULT generateUUIDv4(),
                    status          String        DEFAULT 'pending',
                    format          String        DEFAULT 'csv',
                    created_at      DateTime64(3) DEFAULT now64(3),
                    completed_at    DateTime64(3) DEFAULT toDateTime64(0, 3),
                    download_url    String        DEFAULT '',
                    error_message   String        DEFAULT '',
                    file_size_bytes Int64         DEFAULT 0
                ) ENGINE = ReplacingMergeTree(created_at) ORDER BY (export_id)""",
                    // ── Materialized views (plain таблицы в тестах) ────────────────
                    """CREATE TABLE IF NOT EXISTS audit.mv_daily_msg_type_stats (
                    day      Date,
                    msg_type String,
                    cnt      Int64 DEFAULT 0
                ) ENGINE = SummingMergeTree() ORDER BY (day, msg_type)""",
                    """CREATE TABLE IF NOT EXISTS audit.mv_daily_user_activity (
                    day         Date,
                    usr_id      String,
                    event_count Int64 DEFAULT 0
                ) ENGINE = SummingMergeTree() ORDER BY (day, usr_id)""",
                    """CREATE TABLE IF NOT EXISTS audit.mv_hourly_load_stats (
                    hour_ts     DateTime,
                    msg_type    String,
                    event_count Int64 DEFAULT 0
                ) ENGINE = SummingMergeTree() ORDER BY (hour_ts, msg_type)""",
                    """CREATE TABLE IF NOT EXISTS audit.mv_daily_message_stats (
                    day      Date,
                    msg_type String,
                    cnt      Int64 DEFAULT 0
                ) ENGINE = SummingMergeTree() ORDER BY (day, msg_type)""",
                )

            for (sql in statements) {
                execHttp(baseUrl, sql)
            }

            println("ClickHouse test schema initialized at $baseUrl")
        }

        @DynamicPropertySource
        @JvmStatic
        fun overrideProperties(registry: DynamicPropertyRegistry) {
            registry.add("clickhouse.url") {
                "jdbc:clickhouse://${clickHouse.host}:${clickHouse.getMappedPort(CLICKHOUSE_HTTP_PORT)}/audit?compress=0"
            }
            registry.add("clickhouse.user") { CH_USER }
            registry.add("clickhouse.password") { CH_PASSWORD }
            registry.add("spring.security.oauth2.resourceserver.jwt.issuer-uri") {
                "http://localhost:9999/realms/test"
            }
            registry.add("spring.flyway.enabled") { "false" }
        }
    }
}
