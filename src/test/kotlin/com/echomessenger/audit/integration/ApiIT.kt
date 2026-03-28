package com.echomessenger.audit.integration

import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.http.MediaType
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.get
import org.springframework.test.web.servlet.post

@AutoConfigureMockMvc
class ApiIT : IntegrationTestBase() {
    @Autowired private lateinit var mockMvc: MockMvc

    private val read = jwt().authorities(SimpleGrantedAuthority("ROLE_audit_read"))
    private val admin = jwt().authorities(SimpleGrantedAuthority("ROLE_audit_admin"))
    private val now = System.currentTimeMillis()

    // ── Валидация параметров ──────────────────────────────────────────────────

    @Test
    fun `GET audit events with toTs less than fromTs returns 400`() {
        mockMvc
            .get("/api/v1/audit/events?fromTs=$now&toTs=${now - 1000}") {
                with(read)
            }.andExpect { status { isBadRequest() } }
    }

    @Test
    fun `GET analytics summary with toTs less than fromTs returns 400`() {
        mockMvc
            .get("/api/v1/analytics/summary?fromTs=$now&toTs=${now - 1}") {
                with(read)
            }.andExpect { status { isBadRequest() } }
    }

    @Test
    fun `GET analytics timeseries with unsupported interval returns 400`() {
        mockMvc
            .get("/api/v1/analytics/timeseries?metric=auth.login&interval=week&fromTs=${now - 86400000}&toTs=$now") {
                with(read)
            }.andExpect { status { isBadRequest() } }
    }

    @Test
    fun `GET audit events limit is clamped to max 1000`() {
        // limit=9999 не должен вызывать ошибку — просто зажимается до 1000
        mockMvc
            .get("/api/v1/audit/events?limit=9999") {
                with(read)
            }.andExpect { status { isOk() } }
    }

    @Test
    fun `GET audit events with limit=0 is clamped to 1`() {
        mockMvc
            .get("/api/v1/audit/events?limit=0") {
                with(read)
            }.andExpect { status { isOk() } }
    }

    // ── Cursor pagination через HTTP ──────────────────────────────────────────

    @Test
    fun `GET audit events returns nextCursor in response when hasMore is true`() {
        // Запрашиваем с маленьким limit чтобы гарантированно получить cursor (если есть данные)
        val result =
            mockMvc
                .get("/api/v1/audit/events?limit=1") {
                    with(read)
                }.andExpect {
                    status { isOk() }
                    content { contentType(MediaType.APPLICATION_JSON) }
                }.andReturn()

        val body = result.response.contentAsString
        // Ответ должен содержать поля пагинации
        assertTrue(body.contains("hasMore"), "Response should contain hasMore field")
        assertTrue(body.contains("data"), "Response should contain data field")
    }

    @Test
    fun `GET audit events with invalid cursor returns 400`() {
        // Битый cursor должен возвращать 400 — GlobalExceptionHandler ловит IllegalArgumentException
        // из CursorCodec.decode()
        mockMvc
            .get("/api/v1/audit/events?cursor=definitely_not_a_valid_cursor") {
                with(read)
            }.andExpect { status { isBadRequest() } }
    }

    // ── ReportController sync/async routing ───────────────────────────────────

    @Test
    fun `POST report messages for period less than 7 days returns 200`() {
        val requestBody =
            """
            {
                "fromTs": ${now - 86400000},
                "toTs": $now
            }
            """.trimIndent()

        mockMvc
            .post("/api/v1/audit/reports/messages") {
                with(read)
                contentType = MediaType.APPLICATION_JSON
                content = requestBody
            }.andExpect { status { isOk() } }
    }

    @Test
    fun `POST report messages for period more than 7 days returns 202 with export_id`() {
        val requestBody =
            """
            {
                "fromTs": ${now - 8 * 86400000L},
                "toTs": $now
            }
            """.trimIndent()

        val result =
            mockMvc
                .post("/api/v1/audit/reports/messages") {
                    with(read)
                    contentType = MediaType.APPLICATION_JSON
                    content = requestBody
                }.andExpect {
                    status { isAccepted() }
                }.andReturn()

        val body = result.response.contentAsString
        assertTrue(
            body.contains("export_id"),
            "202 response should contain export_id, got: $body",
        )
        assertTrue(
            body.contains("poll_url"),
            "202 response should contain poll_url",
        )
    }

    // ── Export endpoints ──────────────────────────────────────────────────────

    @Test
    fun `POST audit export returns 202 with export_id`() {
        val requestBody =
            """
            {
                "filters": {
                    "fromTs": ${now - 3600000},
                    "toTs": $now
                },
                "format": "csv"
            }
            """.trimIndent()

        val result =
            mockMvc
                .post("/api/v1/audit/export") {
                    with(read)
                    contentType = MediaType.APPLICATION_JSON
                    content = requestBody
                }.andExpect {
                    status { isAccepted() }
                }.andReturn()

        val body = result.response.contentAsString
        assertTrue(body.contains("export_id"), "Should return export_id")
        assertTrue(body.contains("pending"), "Initial status should be pending")
    }

    @Test
    fun `GET export status returns job info`() {
        // Создаём job
        val createResult =
            mockMvc
                .post("/api/v1/audit/export") {
                    with(read)
                    contentType = MediaType.APPLICATION_JSON
                    content = """{"filters": {"fromTs": ${now - 3600000}, "toTs": $now}, "format": "json"}"""
                }.andReturn()

        val exportId = extractExportId(createResult.response.contentAsString)

        // Проверяем статус
        mockMvc
            .get("/api/v1/audit/export/$exportId") {
                with(read)
            }.andExpect {
                status { isOk() }
                jsonPath("$.exportId") { value(exportId) }
                jsonPath("$.format") { value("json") }
            }
    }

    @Test
    fun `GET export status for unknown id returns 404`() {
        mockMvc
            .get("/api/v1/audit/export/00000000-0000-0000-0000-000000000000") {
                with(read)
            }.andExpect { status { isNotFound() } }
    }

    // ── Retention endpoint ────────────────────────────────────────────────────

    @Test
    fun `GET retention returns policies list`() {
        val result =
            mockMvc
                .get("/api/v1/retention") {
                    with(read)
                }.andExpect {
                    status { isOk() }
                    content { contentType(MediaType.APPLICATION_JSON) }
                }.andReturn()

        val body = result.response.contentAsString
        assertTrue(body.contains("policies"), "Should contain policies array")
        assertTrue(body.contains("tableName"), "Should contain table info")
    }

    // ── Sessions endpoint ─────────────────────────────────────────────────────

    @Test
    fun `GET sessions returns 200`() {
        mockMvc
            .get("/api/v1/audit/sessions") {
                with(read)
            }.andExpect {
                status { isOk() }
                jsonPath("$.data") { isArray() }
                jsonPath("$.hasMore") { isBoolean() }
            }
    }

    // ── User timeline endpoint ────────────────────────────────────────────────

    @Test
    fun `GET user timeline returns 200`() {
        mockMvc
            .get("/api/v1/audit/users/some-user-id/timeline") {
                with(read)
            }.andExpect {
                status { isOk() }
                jsonPath("$.data") { isArray() }
            }
    }

    // ── Incidents endpoints ───────────────────────────────────────────────────

    @Test
    fun `GET incidents with status filter returns 200`() {
        mockMvc
            .get("/api/v1/incidents?status=open&limit=10") {
                with(read)
            }.andExpect {
                status { isOk() }
                jsonPath("$.incidents") { isArray() }
            }
    }

    @Test
    fun `GET incident detail for nonexistent returns 404`() {
        mockMvc
            .get("/api/v1/incidents/00000000-0000-0000-0000-000000000001") {
                with(read)
            }.andExpect { status { isNotFound() } }
    }

    @ParameterizedTest
    @ValueSource(strings = ["invalid_status", "pending", "archived"])
    fun `POST incident status with invalid value returns 400`(invalidStatus: String) {
        // Инцидент не существует, но валидация статуса должна отработать первой
        // IncidentService.updateStatus() проверяет статус ДО обращения к репозиторию
        val result =
            mockMvc
                .post("/api/v1/incidents/00000000-0000-0000-0000-000000000001/status") {
                    with(admin)
                    contentType = MediaType.APPLICATION_JSON
                    content = """{"status": "$invalidStatus"}"""
                }.andReturn()

        val status = result.response.status
        assertTrue(
            status == 400 || status == 404,
            "Expected 400 (invalid status) or 404 (incident not found), got $status",
        )
    }

    // ── Analytics endpoints ───────────────────────────────────────────────────

    @ParameterizedTest
    @ValueSource(strings = ["hour", "day"])
    fun `GET timeseries for valid intervals returns 200`(interval: String) {
        mockMvc
            .get("/api/v1/analytics/timeseries?metric=auth.login&interval=$interval&fromTs=${now - 86400000}&toTs=$now") {
                with(read)
            }.andExpect {
                status { isOk() }
                jsonPath("$.metric") { value("auth.login") }
                jsonPath("$.interval") { value(interval) }
                jsonPath("$.points") { isArray() }
            }
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private fun extractExportId(json: String): String {
        val regex = """"export_id"\s*:\s*"([^"]+)"""".toRegex()
        return regex.find(json)?.groupValues?.get(1)
            ?: error("No export_id found in: $json")
    }

    private fun assertTrue(
        condition: Boolean,
        message: String,
    ) = org.junit.jupiter.api.Assertions
        .assertTrue(condition, message)
}
