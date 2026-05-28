package com.echomessenger.audit.unit

import com.echomessenger.audit.api.InternalIncidentController
import com.echomessenger.audit.service.IncidentService
import io.mockk.justRun
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.http.HttpStatus

class InternalIncidentControllerTest {
    @Test
    fun `triggerDetection runs detector and returns ok payload`() {
        val incidentService = mockk<IncidentService>()
        justRun { incidentService.runDetection() }
        val controller = InternalIncidentController(incidentService)

        val response = controller.triggerDetection()

        assertEquals(HttpStatus.OK, response.statusCode)
        assertEquals("ok", response.body?.get("status"))
        assertEquals("manual", response.body?.get("trigger"))
        verify(exactly = 1) { incidentService.runDetection() }
    }
}
