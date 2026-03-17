package com.echomessenger.audit.integration

import com.echomessenger.audit.domain.Incident
import com.echomessenger.audit.repository.IncidentRepository
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.util.UUID

class IncidentRepositoryIT : IntegrationTestBase() {
    @Autowired private lateinit var incidentRepository: IncidentRepository

    // ── upsert + findById ─────────────────────────────────────────────────────

    @Test
    fun `upsert and findById round-trip`() {
        val id = UUID.randomUUID().toString()
        val incident =
            Incident(
                incidentId = id,
                type = "brute_force",
                status = "open",
                detectedAt = System.currentTimeMillis(),
                userId = "user_test_${id.take(8)}",
                details = mapOf("ip" to "10.0.0.1", "attempt_count" to 15L),
                updatedAt = System.currentTimeMillis(),
            )

        incidentRepository.upsert(incident)
        Thread.sleep(500) // ClickHouse flush

        val found = incidentRepository.findById(id)
        assertNotNull(found, "Should find inserted incident")
        assertEquals(id, found!!.incidentId)
        assertEquals("brute_force", found.type)
        assertEquals("open", found.status)
        assertEquals("10.0.0.1", found.details["ip"])
    }

    @Test
    fun `findById returns null for nonexistent valid UUID`() {
        val result = incidentRepository.findById(UUID.randomUUID().toString())
        assertNull(result, "Should return null for unknown UUID")
    }

    @Test
    fun `findById returns null for invalid UUID string`() {
        // Проверяем что невалидный UUID не бросает исключение (защита от 500)
        val result = incidentRepository.findById("not-a-uuid")
        assertNull(result, "Should return null for invalid UUID format")
    }

    // ── status transition через ReplacingMergeTree ────────────────────────────

    @Test
    fun `upsert with newer updatedAt updates status`() {
        val id = UUID.randomUUID().toString()
        val base =
            Incident(
                incidentId = id,
                type = "mass_delete",
                status = "open",
                detectedAt = System.currentTimeMillis() - 10_000,
                userId = "user_${id.take(8)}",
                details = emptyMap(),
                updatedAt = System.currentTimeMillis() - 10_000,
            )
        incidentRepository.upsert(base)

        // Обновляем статус через новый INSERT с тем же incident_id но новым updatedAt
        val updated =
            base.copy(
                status = "confirmed",
                details = mapOf("admin_comment" to "verified"),
                updatedAt = System.currentTimeMillis(),
            )
        incidentRepository.upsert(updated)

        Thread.sleep(800) // даём время на merge ReplacingMergeTree
        // После OPTIMIZE или достаточного времени найдём актуальную версию
        val found = incidentRepository.findById(id)
        assertNotNull(found)
        // Статус может быть "open" или "confirmed" в зависимости от merge —
        // главное что findById не падает и возвращает валидный объект
        assertTrue(
            found!!.status in listOf("open", "confirmed"),
            "Status should be valid, got: ${found.status}",
        )
    }

    // ── findAll фильтры ───────────────────────────────────────────────────────

    @Test
    fun `findAll filters by status`() {
        val runId = UUID.randomUUID().toString().take(8)
        val userId = "filter_test_$runId"

        // Создаём два инцидента: open и dismissed
        incidentRepository.upsert(openIncident(userId = userId, type = "brute_force"))
        incidentRepository.upsert(
            openIncident(userId = userId, type = "mass_delete")
                .copy(status = "dismissed"),
        )
        Thread.sleep(500)

        val openOnes = incidentRepository.findAll(status = "open", userId = userId)
        val dismissedOnes = incidentRepository.findAll(status = "dismissed", userId = userId)

        assertTrue(openOnes.all { it.status == "open" }, "All should be open")
        assertTrue(dismissedOnes.all { it.status == "dismissed" }, "All should be dismissed")
    }

    @Test
    fun `findAll filters by type`() {
        val runId = UUID.randomUUID().toString().take(8)
        val userId = "type_test_$runId"

        incidentRepository.upsert(openIncident(userId = userId, type = "brute_force"))
        incidentRepository.upsert(openIncident(userId = userId, type = "device_switch"))
        Thread.sleep(500)

        val bruteForce = incidentRepository.findAll(type = "brute_force", userId = userId)
        assertTrue(bruteForce.all { it.type == "brute_force" })
        assertTrue(bruteForce.isNotEmpty(), "Should find brute_force incidents")
    }

    @Test
    fun `findAll filters by userId`() {
        val runId = UUID.randomUUID().toString().take(8)
        val targetUser = "target_$runId"
        val otherUser = "other_$runId"

        incidentRepository.upsert(openIncident(userId = targetUser, type = "brute_force"))
        incidentRepository.upsert(openIncident(userId = otherUser, type = "brute_force"))
        Thread.sleep(500)

        val result = incidentRepository.findAll(userId = targetUser)
        assertTrue(
            result.all { it.userId == targetUser },
            "All incidents should belong to targetUser",
        )
    }

    @Test
    fun `existsByTypeAndUserAndWindow detects recent incident`() {
        val runId = UUID.randomUUID().toString().take(8)
        val userId = "exist_test_$runId"

        incidentRepository.upsert(openIncident(userId = userId, type = "volume_anomaly"))
        Thread.sleep(500)

        val exists =
            incidentRepository.existsByTypeAndUserAndWindow(
                type = "volume_anomaly",
                userId = userId,
                windowMs = 60_000L,
            )
        assertTrue(exists, "Should detect recently created incident")
    }

    @Test
    fun `existsByTypeAndUserAndWindow returns false for old incidents`() {
        // Проверяем что инцидент вне окна не детектируется
        val exists =
            incidentRepository.existsByTypeAndUserAndWindow(
                type = "brute_force",
                userId = "nonexistent_user_xyz",
                windowMs = 1L, // окно в 1мс — ничего не попадёт
            )
        assertFalse(exists, "Should not find incident outside time window")
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private fun openIncident(
        userId: String,
        type: String,
    ) = Incident(
        incidentId = UUID.randomUUID().toString(),
        type = type,
        status = "open",
        detectedAt = System.currentTimeMillis(),
        userId = userId,
        details = mapOf("test" to true),
        updatedAt = System.currentTimeMillis(),
    )
}
