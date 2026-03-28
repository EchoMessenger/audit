package com.echomessenger.audit.unit

import com.echomessenger.audit.domain.MessageReportItem
import com.echomessenger.audit.domain.MessageReportRequest
import com.echomessenger.audit.repository.MessageRepository
import com.echomessenger.audit.service.ReportService
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

class ReportServiceTest {
    private val messageRepository: MessageRepository = mockk()
    private val service = ReportService(messageRepository)

    private val now = System.currentTimeMillis()
    private val dayMs = TimeUnit.DAYS.toMillis(1)

    // ── requiresAsyncExport boundary ──────────────────────────────────────────

    @Test
    fun `period of exactly 7 days does not require async`() {
        val req = request(fromTs = now - 7 * dayMs, toTs = now)
        assertFalse(
            service.requiresAsyncExport(req),
            "Exactly 7 days should be synchronous",
        )
    }

    @Test
    fun `period of 7 days + 1ms requires async`() {
        val req = request(fromTs = now - 7 * dayMs - 1, toTs = now)
        assertTrue(
            service.requiresAsyncExport(req),
            "7 days + 1ms should require async export",
        )
    }

    @Test
    fun `period of 1 day does not require async`() {
        val req = request(fromTs = now - dayMs, toTs = now)
        assertFalse(service.requiresAsyncExport(req))
    }

    @Test
    fun `period of 30 days requires async`() {
        val req = request(fromTs = now - 30 * dayMs, toTs = now)
        assertTrue(service.requiresAsyncExport(req))
    }

    // ── generateReport ────────────────────────────────────────────────────────

    @Test
    fun `generateReport returns report with correct metadata`() {
        val req = request(fromTs = now - dayMs, toTs = now)
        every { messageRepository.findMessages(req, any()) } returns
            listOf(
                messageItem(1L),
                messageItem(2L),
                messageItem(3L),
            )

        val report = service.generateReport(req)

        assertNotNull(report.reportId)
        assertEquals(3L, report.totalMessages)
        assertEquals(3, report.messages.size)
        assertTrue(report.generatedAt > 0)
    }

    @Test
    fun `generateReport calls repository with correct limit`() {
        val req = request(fromTs = now - dayMs, toTs = now)
        every { messageRepository.findMessages(req, 10_000) } returns emptyList()

        service.generateReport(req)

        verify { messageRepository.findMessages(req, 10_000) }
    }

    @Test
    fun `generateReport throws for period exceeding 7 days`() {
        val req = request(fromTs = now - 8 * dayMs, toTs = now)

        assertThrows(IllegalArgumentException::class.java) {
            service.generateReport(req)
        }
    }

    @Test
    fun `generateReport with empty result returns zero messages`() {
        val req = request(fromTs = now - dayMs, toTs = now)
        every { messageRepository.findMessages(req, any()) } returns emptyList()

        val report = service.generateReport(req)

        assertEquals(0L, report.totalMessages)
        assertTrue(report.messages.isEmpty())
    }

    @Test
    fun `generateReport with user and topic filters passes them through`() {
        val req =
            request(
                fromTs = now - dayMs,
                toTs = now,
                users = listOf("user1"),
                topics = listOf("topic1"),
            )
        every { messageRepository.findMessages(req, any()) } returns emptyList()

        service.generateReport(req)

        verify {
            messageRepository.findMessages(
                match { r ->
                    r.users == listOf("user1") && r.topics == listOf("topic1")
                },
                any(),
            )
        }
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private fun request(
        fromTs: Long,
        toTs: Long,
        users: List<String>? = null,
        topics: List<String>? = null,
    ) = MessageReportRequest(
        fromTs = fromTs,
        toTs = toTs,
        users = users,
        topics = topics,
    )

    private fun messageItem(id: Long) =
        MessageReportItem(
            messageId = id,
            topicId = "topic1",
            userId = "user1",
            userName = "User One",
            timestamp = System.currentTimeMillis(),
            content = "test message $id",
            isDeleted = false,
        )
}
