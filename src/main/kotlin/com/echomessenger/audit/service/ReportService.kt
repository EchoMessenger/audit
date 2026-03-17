package com.echomessenger.audit.service

import com.echomessenger.audit.domain.MessageReport
import com.echomessenger.audit.domain.MessageReportRequest
import com.echomessenger.audit.repository.MessageRepository
import org.springframework.stereotype.Service
import java.util.UUID
import java.util.concurrent.TimeUnit

@Service
class ReportService(
    private val messageRepository: MessageRepository,
) {
    companion object {
        // Порог: если период > 7 дней — редиректим на async export
        val SYNC_MAX_DAYS = 7L
        val SYNC_MAX_MS = TimeUnit.DAYS.toMillis(SYNC_MAX_DAYS)
    }

    /**
     * Определяет, нужен ли async export на основе размера диапазона.
     */
    fun requiresAsyncExport(req: MessageReportRequest): Boolean = (req.toTs - req.fromTs) > SYNC_MAX_MS

    /**
     * Синхронная генерация отчёта (только для периодов <= 7 дней).
     */
    fun generateReport(req: MessageReportRequest): MessageReport {
        require(!requiresAsyncExport(req)) {
            "Period exceeds ${SYNC_MAX_DAYS}d — use async export"
        }

        val messages = messageRepository.findMessages(req, limit = 10_000)
        return MessageReport(
            reportId = UUID.randomUUID().toString(),
            generatedAt = System.currentTimeMillis(),
            totalMessages = messages.size.toLong(),
            messages = messages,
        )
    }
}
