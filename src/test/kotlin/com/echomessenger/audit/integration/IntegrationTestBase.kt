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
        private const val CLICKHOUSE_TCP_PORT = 9000
        private const val CH_USER = "default"
        private const val CH_PASSWORD = "test_secret"

        private val clickHouse: GenericContainer<*> =
            GenericContainer(
                DockerImageName.parse("clickhouse/clickhouse-server:24.3-alpine"),
            ).withExposedPorts(CLICKHOUSE_HTTP_PORT, CLICKHOUSE_TCP_PORT)
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

        private fun execHttp(baseUrl: String, sql: String) {
            val client = HttpClient.newHttpClient()
            val uri = URI.create("$baseUrl/?user=$CH_USER&password=$CH_PASSWORD")
            val request = HttpRequest.newBuilder()
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

            val statements = listOf(
                // ============================================
                // DATABASE
                // ============================================
                "CREATE DATABASE IF NOT EXISTS audit",

                // ============================================
                // ОСНОВНЫЕ ТАБЛИЦЫ
                // ============================================

                // ── account_log ──
                """CREATE TABLE IF NOT EXISTS audit.account_log (
                    log_id        UUID          DEFAULT generateUUIDv4(),
                    log_timestamp DateTime64(3) DEFAULT now64(3),
                    action        Enum8('CREATE' = 0, 'UPDATE' = 1, 'DELETE' = 2),
                    user_id       String,
                    default_acs_auth Nullable(String),
                    default_acs_anon Nullable(String),
                    public        Nullable(String),
                    tags          Array(String)
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(log_timestamp)
                ORDER BY (log_timestamp, user_id, action)""",

                // ── topic_log ──
                """CREATE TABLE IF NOT EXISTS audit.topic_log (
                    log_id        UUID          DEFAULT generateUUIDv4(),
                    log_timestamp DateTime64(3) DEFAULT now64(3),
                    action        Enum8('CREATE' = 0, 'UPDATE' = 1, 'DELETE' = 2),
                    topic_name    String,
                    desc_created_at        Nullable(Int64),
                    desc_updated_at        Nullable(Int64),
                    desc_touched_at        Nullable(Int64),
                    desc_defacs_auth       Nullable(String),
                    desc_defacs_anon       Nullable(String),
                    desc_acs_want          Nullable(String),
                    desc_acs_given         Nullable(String),
                    desc_seq_id            Nullable(Int32),
                    desc_read_id           Nullable(Int32),
                    desc_recv_id           Nullable(Int32),
                    desc_del_id            Nullable(Int32),
                    desc_public            Nullable(String),
                    desc_private           Nullable(String),
                    desc_trusted           Nullable(String),
                    desc_state             Nullable(String),
                    desc_state_at          Nullable(Int64),
                    desc_is_chan           Nullable(Bool),
                    desc_online            Nullable(Bool),
                    desc_last_seen_time    Nullable(Int64),
                    desc_last_seen_user_agent Nullable(String)
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(log_timestamp)
                ORDER BY (log_timestamp, topic_name, action)""",

                // ── subscription_log ──
                """CREATE TABLE IF NOT EXISTS audit.subscription_log (
                    log_id        UUID          DEFAULT generateUUIDv4(),
                    log_timestamp DateTime64(3) DEFAULT now64(3),
                    action        Enum8('CREATE' = 0, 'UPDATE' = 1, 'DELETE' = 2),
                    topic         String,
                    user_id       String,
                    del_id        Nullable(Int32),
                    read_id       Nullable(Int32),
                    recv_id       Nullable(Int32),
                    mode_want     Nullable(String),
                    mode_given    Nullable(String),
                    private       Nullable(String)
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(log_timestamp)
                ORDER BY (log_timestamp, topic, user_id, action)""",

                // ── message_log ──
                """CREATE TABLE IF NOT EXISTS audit.message_log (
                    log_id           UUID          DEFAULT generateUUIDv4(),
                    log_timestamp    DateTime64(3) DEFAULT now64(3),
                    action           Enum8('CREATE' = 0, 'UPDATE' = 1, 'DELETE' = 2),
                    msg_topic        String,
                    msg_from_user_id Nullable(String),
                    msg_timestamp    Int64,
                    msg_deleted_at   Nullable(Int64),
                    msg_seq_id       Int32,
                    msg_head         Map(String, String),
                    msg_content      Nullable(String)
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(log_timestamp)
                ORDER BY (log_timestamp, msg_topic, msg_seq_id)""",

                // ── client_req_log ──
                """CREATE TABLE IF NOT EXISTS audit.client_req_log (
                    log_id           UUID          DEFAULT generateUUIDv4(),
                    log_timestamp    DateTime64(3) DEFAULT now64(3),
                    sess_session_id  String        DEFAULT '',
                    sess_user_id     String        DEFAULT '',
                    sess_auth_level  String        DEFAULT '',
                    sess_remote_addr String        DEFAULT '',
                    sess_user_agent  String        DEFAULT '',
                    sess_device_id   String        DEFAULT '',
                    sess_language    String        DEFAULT '',
                    msg_type         String        DEFAULT '',
                    msg_id           String        DEFAULT '',
                    msg_topic        String        DEFAULT '',
                    extra_attachments  Array(String) DEFAULT [],
                    extra_on_behalf_of Nullable(String),
                    extra_auth_level   Nullable(String),
                    hi_user_agent    Nullable(String),
                    hi_ver           Nullable(String),
                    hi_device_id     Nullable(String),
                    hi_lang          Nullable(String),
                    hi_platform      Nullable(String),
                    hi_background    Nullable(Bool),
                    acc_user_id      Nullable(String),
                    acc_scheme       Nullable(String),
                    acc_login        Nullable(Bool),
                    acc_state        Nullable(String),
                    acc_auth_level   Nullable(String),
                    acc_tmp_scheme   Nullable(String),
                    acc_tags         Array(String) DEFAULT [],
                    login_scheme     Nullable(String),
                    sub_topic        Nullable(String),
                    leave_unsub      Nullable(Bool),
                    pub_no_echo      Nullable(Bool),
                    pub_head         Map(String, String) DEFAULT map(),
                    pub_content      Nullable(String),
                    get_what         Nullable(String),
                    set_topic        Nullable(String),
                    del_what         Nullable(String),
                    del_user_id      Nullable(String),
                    del_hard         Nullable(Bool),
                    note_what        Nullable(String),
                    note_seq_id      Nullable(Int32),
                    note_event       Nullable(String)
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(log_timestamp)
                ORDER BY (log_timestamp, sess_user_id, msg_type)""",

                // ── search_log ──
                """CREATE TABLE IF NOT EXISTS audit.search_log (
                    log_id        UUID          DEFAULT generateUUIDv4(),
                    log_timestamp DateTime64(3) DEFAULT now64(3),
                    user_id       String,
                    query         String
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(log_timestamp)
                ORDER BY (log_timestamp, user_id)""",

                // ── incident_log ──
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

                // ── export_job_log ──
                """CREATE TABLE IF NOT EXISTS audit.export_job_log (
                    export_id       UUID          DEFAULT generateUUIDv4(),
                    status          String        DEFAULT 'pending',
                    format          String        DEFAULT 'csv',
                    created_at      DateTime64(3) DEFAULT now64(3),
                    completed_at    DateTime64(3) DEFAULT toDateTime64(0, 3),
                    download_url    String        DEFAULT '',
                    error_message   String        DEFAULT '',
                    file_size_bytes Int64         DEFAULT 0,
                    row_version     UInt64        DEFAULT 0
                ) ENGINE = ReplacingMergeTree(row_version) ORDER BY (export_id)""",

                // ============================================
                // TARGET TABLES для Materialized Views
                // ============================================

                """CREATE TABLE IF NOT EXISTS audit.mv_daily_msg_type_stats_store (
                    day      Date,
                    msg_type String,
                    cnt      UInt64
                ) ENGINE = SummingMergeTree()
                PARTITION BY toYYYYMM(day)
                ORDER BY (day, msg_type)""",

                """CREATE TABLE IF NOT EXISTS audit.mv_daily_user_activity_store (
                    day         Date,
                    usr_id      String,
                    event_count UInt64
                ) ENGINE = SummingMergeTree()
                PARTITION BY toYYYYMM(day)
                ORDER BY (day, usr_id)""",

                """CREATE TABLE IF NOT EXISTS audit.mv_hourly_load_stats_store (
                    hour_ts     DateTime,
                    msg_type    String,
                    event_count UInt64
                ) ENGINE = SummingMergeTree()
                PARTITION BY toYYYYMM(hour_ts)
                ORDER BY (hour_ts, msg_type)""",

                // ============================================
                // MATERIALIZED VIEWS (триггеры на INSERT)
                // ============================================

                """CREATE MATERIALIZED VIEW IF NOT EXISTS audit.mv_daily_msg_type_stats_writer
                TO audit.mv_daily_msg_type_stats_store
                AS SELECT
                    toDate(log_timestamp) AS day,
                    msg_type,
                    count() AS cnt
                FROM audit.client_req_log
                GROUP BY day, msg_type""",

                """CREATE MATERIALIZED VIEW IF NOT EXISTS audit.mv_daily_user_activity_writer
                TO audit.mv_daily_user_activity_store
                AS SELECT
                    toDate(log_timestamp) AS day,
                    sess_user_id AS usr_id,
                    count() AS event_count
                FROM audit.client_req_log
                WHERE sess_user_id != ''
                GROUP BY day, usr_id""",

                """CREATE MATERIALIZED VIEW IF NOT EXISTS audit.mv_hourly_load_stats_writer
                TO audit.mv_hourly_load_stats_store
                AS SELECT
                    toStartOfHour(log_timestamp) AS hour_ts,
                    msg_type,
                    count() AS event_count
                FROM audit.client_req_log
                GROUP BY hour_ts, msg_type""",

                // ============================================
                // VIEW-АЛИАСЫ для чтения (совместимость с репозиторием)
                // ============================================

                """CREATE VIEW IF NOT EXISTS audit.mv_daily_msg_type_stats AS
                SELECT day, msg_type, sum(cnt) AS cnt
                FROM audit.mv_daily_msg_type_stats_store
                GROUP BY day, msg_type""",

                """CREATE VIEW IF NOT EXISTS audit.mv_daily_user_activity AS
                SELECT day, usr_id, sum(event_count) AS event_count
                FROM audit.mv_daily_user_activity_store
                GROUP BY day, usr_id""",

                """CREATE VIEW IF NOT EXISTS audit.mv_hourly_load_stats AS
                SELECT hour_ts, msg_type, sum(event_count) AS event_count
                FROM audit.mv_hourly_load_stats_store
                GROUP BY hour_ts, msg_type""",
            )

            for (sql in statements) {
                execHttp(baseUrl, sql)
            }

            println("✓ ClickHouse test schema initialized at $baseUrl")
        }

        @DynamicPropertySource
        @JvmStatic
        fun overrideProperties(registry: DynamicPropertyRegistry) {
            registry.add("clickhouse.http-url") {
                "http://${clickHouse.host}:${clickHouse.getMappedPort(CLICKHOUSE_HTTP_PORT)}"
            }
            registry.add("clickhouse.username") { CH_USER }
            registry.add("clickhouse.password") { CH_PASSWORD }
            registry.add("spring.security.oauth2.resourceserver.jwt.issuer-uri") {
                "http://localhost:9999/realms/test"
            }
            registry.add("spring.flyway.enabled") { "false" }
        }
    }
}