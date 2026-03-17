package com.echomessenger.audit.integration

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.http.MediaType
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.get
import org.springframework.test.web.servlet.post

@AutoConfigureMockMvc
class SecurityIT : IntegrationTestBase() {
    @Autowired
    private lateinit var mockMvc: MockMvc

    // Переиспользуемые процессоры — явный SimpleGrantedAuthority снимает ambiguity
    // между vararg GrantedAuthority и Converter<Jwt, Collection<GrantedAuthority>>
    private val auditRead = jwt().authorities(SimpleGrantedAuthority("ROLE_audit_read"))
    private val auditAdmin = jwt().authorities(SimpleGrantedAuthority("ROLE_audit_admin"))

    // ── 401 без токена ────────────────────────────────────────────────────────

    @Test
    fun `GET audit events without token returns 401`() {
        mockMvc
            .get("/api/v1/audit/events")
            .andExpect { status { isUnauthorized() } }
    }

    @Test
    fun `GET incidents without token returns 401`() {
        mockMvc
            .get("/api/v1/incidents")
            .andExpect { status { isUnauthorized() } }
    }

    // ── 200 с audit_read ──────────────────────────────────────────────────────

    @Test
    fun `GET audit events with audit_read role returns 200`() {
        mockMvc
            .get("/api/v1/audit/events") {
                with(auditRead)
            }.andExpect { status { isOk() } }
    }

    @Test
    fun `GET analytics summary with audit_read returns 200`() {
        val now = System.currentTimeMillis()
        mockMvc
            .get("/api/v1/analytics/summary?fromTs=${now - 86400000}&toTs=$now") {
                with(auditRead)
            }.andExpect { status { isOk() } }
    }

    @Test
    fun `GET incidents with audit_read returns 200`() {
        mockMvc
            .get("/api/v1/incidents") {
                with(auditRead)
            }.andExpect { status { isOk() } }
    }

    // ── 403 при попытке admin-операции с audit_read ───────────────────────────

    @Test
    fun `POST incident status with audit_read returns 403`() {
        mockMvc
            .post("/api/v1/incidents/some-id/status") {
                with(auditRead)
                contentType = MediaType.APPLICATION_JSON
                content = """{"status": "confirmed"}"""
            }.andExpect { status { isForbidden() } }
    }

    // ── 404 admin-операция с audit_admin (security прошёл, инцидента нет) ─────

    @Test
    fun `POST incident status with audit_admin returns 404 for nonexistent incident`() {
        mockMvc
            .post("/api/v1/incidents/nonexistent-id/status") {
                with(auditAdmin)
                contentType = MediaType.APPLICATION_JSON
                content = """{"status": "confirmed", "comment": "test"}"""
            }.andExpect { status { isNotFound() } }
    }

    // ── Actuator доступен без токена ──────────────────────────────────────────

    @Test
    fun `actuator health available without token`() {
        mockMvc
            .get("/actuator/health")
            .andExpect { status { isOk() } }
    }

    // ── Rate limit 429 после 5 запросов ──────────────────────────────────────

    @Test
    fun `POST report messages returns 429 after rate limit exceeded`() {
        // Уникальный subject чтобы не конфликтовать с другими тестами в том же bucket
        val uniqueJwt =
            jwt()
                .jwt { it.subject("rate-limit-test-${System.nanoTime()}") }
                .authorities(SimpleGrantedAuthority("ROLE_audit_read"))

        val requestBody =
            """
            {"fromTs": ${System.currentTimeMillis() - 3_600_000}, "toTs": ${System.currentTimeMillis()}}
            """.trimIndent()

        // Исчерпываем лимит (5 запросов/минуту)
        repeat(5) {
            mockMvc.post("/api/v1/audit/reports/messages") {
                with(uniqueJwt)
                contentType = MediaType.APPLICATION_JSON
                content = requestBody
            }
        }

        // Шестой должен получить 429 с заголовком retry-after
        mockMvc
            .post("/api/v1/audit/reports/messages") {
                with(uniqueJwt)
                contentType = MediaType.APPLICATION_JSON
                content = requestBody
            }.andExpect {
                status { isTooManyRequests() }
                header { exists("X-Rate-Limit-Retry-After-Seconds") }
            }
    }
}
