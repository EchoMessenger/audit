package com.echomessenger.audit.unit

import com.echomessenger.audit.domain.Incident
import com.echomessenger.audit.repository.IncidentRepository
import com.echomessenger.audit.service.IncidentService
import io.mockk.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate

class IncidentServiceTest {
    private val incidentRepository: IncidentRepository = mockk()
    private val jdbc: NamedParameterJdbcTemplate = mockk()

    private val service =
        IncidentService(
            incidentRepository = incidentRepository,
            jdbc = jdbc,
            bruteForceThreshold = 10,
            bruteForceWindowMinutes = 5,
            massDeleteThreshold = 5,
            massDeleteWindowSeconds = 60,
            volumeAnomalyMultiplier = 10.0,
        )

    @BeforeEach
    fun setUp() {
        clearAllMocks()
    }

    // ── updateStatus ──────────────────────────────────────────────────────────

    @Test
    fun `updateStatus transitions open to confirmed`() {
        val incident = openIncident()
        every { incidentRepository.findById("inc-1") } returns incident
        every { incidentRepository.upsert(any()) } just Runs

        val result = service.updateStatus("inc-1", "confirmed", "Verified by admin")

        assertEquals("confirmed", result.status)
        assertEquals("Verified by admin", result.details["admin_comment"])
        verify { incidentRepository.upsert(match { it.status == "confirmed" }) }
    }

    @Test
    fun `updateStatus transitions open to dismissed`() {
        val incident = openIncident()
        every { incidentRepository.findById("inc-1") } returns incident
        every { incidentRepository.upsert(any()) } just Runs

        val result = service.updateStatus("inc-1", "dismissed", null)

        assertEquals("dismissed", result.status)
    }

    @Test
    fun `updateStatus throws for invalid status`() {
        assertThrows<IllegalArgumentException> {
            service.updateStatus("inc-1", "invalid_status", null)
        }
        verify(exactly = 0) { incidentRepository.findById(any()) }
    }

    @Test
    fun `updateStatus throws NoSuchElementException when incident not found`() {
        every { incidentRepository.findById("nonexistent") } returns null

        assertThrows<NoSuchElementException> {
            service.updateStatus("nonexistent", "confirmed", null)
        }
    }

    @Test
    fun `updateStatus preserves existing details`() {
        val incident = openIncident(details = mapOf("ip" to "1.2.3.4", "attempt_count" to 15L))
        every { incidentRepository.findById("inc-1") } returns incident
        every { incidentRepository.upsert(any()) } just Runs

        val result = service.updateStatus("inc-1", "confirmed", "Confirmed")

        // Оригинальные details сохранены
        assertEquals("1.2.3.4", result.details["ip"])
        assertEquals(15L, result.details["attempt_count"])
        assertEquals("Confirmed", result.details["admin_comment"])
    }

    // ── listIncidents ─────────────────────────────────────────────────────────

    @Test
    fun `listIncidents delegates to repository with all filters`() {
        every {
            incidentRepository.findAll(
                status = "open",
                type = "brute_force",
                userId = "user1",
                limit = 50,
            )
        } returns listOf(openIncident())

        val result = service.listIncidents("open", "brute_force", "user1", 50)

        assertEquals(1, result.size)
        verify { incidentRepository.findAll("open", "brute_force", "user1", 50) }
    }

    @Test
    fun `listIncidents with null filters`() {
        every { incidentRepository.findAll(null, null, null, 100) } returns emptyList()

        val result = service.listIncidents(null, null, null, 100)

        assertTrue(result.isEmpty())
    }

    // ── getIncident ───────────────────────────────────────────────────────────

    @Test
    fun `getIncident returns null for unknown id`() {
        every { incidentRepository.findById("unknown") } returns null

        val result = service.getIncident("unknown")

        assertNull(result)
    }

    @Test
    fun `getIncident returns existing incident`() {
        val incident = openIncident()
        every { incidentRepository.findById("inc-1") } returns incident

        val result = service.getIncident("inc-1")

        assertNotNull(result)
        assertEquals("inc-1", result!!.incidentId)
        assertEquals("brute_force", result.type)
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private fun openIncident(details: Map<String, Any?> = mapOf("ip" to "10.0.0.1", "attempt_count" to 12L)) =
        Incident(
            incidentId = "inc-1",
            type = "brute_force",
            status = "open",
            detectedAt = System.currentTimeMillis() - 60_000,
            userId = null,
            details = details,
            updatedAt = System.currentTimeMillis() - 60_000,
        )
}
