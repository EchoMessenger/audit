package com.echomessenger.audit.service

import com.echomessenger.audit.domain.*
import com.echomessenger.audit.repository.ExportRepository
import com.echomessenger.audit.repository.MessageRepository
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.util.UUID

@Service
class ExportService(
    private val exportRepository: ExportRepository,
    private val messageRepository: MessageRepository,
    private val userNameResolver: UserNameResolver,
    @Value("\${audit.export.storage-type:pvc}") private val storageType: String,
    @Value("\${audit.export.pvc-path:/exports}") private val pvcPath: String,
) {
    private val log = LoggerFactory.getLogger(ExportService::class.java)
    private val mapper = jacksonObjectMapper()

    // Выделенный scope для фоновых export задач — SupervisorJob не роняет весь scope при ошибке
    private val exportScope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    /**
     * Создаёт ExportJob в статусе pending, запускает фоновую coroutine.
     * Возвращает job немедленно — не ждёт завершения.
     */
    fun startExport(request: ExportRequest): ExportJob {
        val job =
            ExportJob(
                exportId = UUID.randomUUID().toString(),
                status = ExportStatus.pending,
                format = request.format,
                createdAt = System.currentTimeMillis(),
                completedAt = null,
                downloadUrl = null,
                errorMessage = null,
                fileSizeBytes = null,
            )
        exportRepository.save(job)

        exportScope.launch {
            runExport(job, request)
        }

        return job
    }

    fun getJob(exportId: String): ExportJob? = exportRepository.findById(exportId)

    // ── Internal ──────────────────────────────────────────────────────────────

    private suspend fun runExport(
        job: ExportJob,
        request: ExportRequest,
    ) {
        log.info("Starting export exportId={} format={}", job.exportId, job.format)

        // Обновляем статус на running
        exportRepository.save(job.copy(status = ExportStatus.running))

        val outputFile = resolveOutputFile(job.exportId, job.format)
        outputFile.parentFile?.mkdirs()

        try {
            val messageReportReq = request.filters.toMessageReportRequest()

            BufferedWriter(FileWriter(outputFile)).use { writer ->
                when (job.format) {
                    ExportFormat.csv -> exportCsv(writer, messageReportReq)
                    ExportFormat.json -> exportJson(writer, messageReportReq)
                }
            }

            val downloadUrl = resolveDownloadUrl(job.exportId, job.format)
            exportRepository.save(
                job.copy(
                    status = ExportStatus.completed,
                    completedAt = System.currentTimeMillis(),
                    downloadUrl = downloadUrl,
                    fileSizeBytes = outputFile.length(),
                ),
            )
            log.info("Export completed exportId={} bytes={}", job.exportId, outputFile.length())
        } catch (e: Exception) {
            log.error("Export failed exportId={}", job.exportId, e)
            exportRepository.save(
                job.copy(
                    status = ExportStatus.failed,
                    completedAt = System.currentTimeMillis(),
                    errorMessage = e.message?.take(500),
                ),
            )
            outputFile.delete()
        }
    }

    private fun exportCsv(
        writer: BufferedWriter,
        req: MessageReportRequest,
    ) {
        // Header
        writer.write("message_id,topic_id,user_id,user_name,timestamp,content,is_deleted")
        writer.newLine()

        messageRepository.streamMessages(req, batchSize = 1000) { batch ->
            val enriched = userNameResolver.enrichMissingUserNames(batch)
            enriched.forEach { msg ->
                val content =
                    msg.content
                        ?.replace("\"", "\"\"") // CSV escaping
                        ?.let { "\"$it\"" }
                        ?: ""
                val userName = msg.userName?.replace("\"", "\"\"")?.let { "\"$it\"" } ?: ""
                writer.write("${msg.messageId},${msg.topicId},${msg.userId ?: ""},$userName,${msg.timestamp},$content,${msg.isDeleted}")
                writer.newLine()
            }
        }
    }

    private fun exportJson(
        writer: BufferedWriter,
        req: MessageReportRequest,
    ) {
        writer.write("[")
        var first = true

        messageRepository.streamMessages(req, batchSize = 1000) { batch ->
            val enriched = userNameResolver.enrichMissingUserNames(batch)
            enriched.forEach { msg ->
                if (!first) writer.write(",")
                writer.newLine()
                writer.write(mapper.writeValueAsString(msg))
                first = false
            }
        }

        writer.newLine()
        writer.write("]")
    }

    private fun ExportFilters.toMessageReportRequest(): MessageReportRequest {
        val userList = users?.takeIf { it.isNotEmpty() } ?: userId?.let { listOf(it) }
        val topicList = topics?.takeIf { it.isNotEmpty() } ?: topicId?.let { listOf(it) }
        return MessageReportRequest(
            users = userList,
            topics = topicList,
            fromTs = fromTs ?: 0L,
            toTs = toTs ?: System.currentTimeMillis(),
            includeDeleted = includeDeleted ?: false,
        )
    }

    private fun resolveOutputFile(
        exportId: String,
        format: ExportFormat,
    ): File =
        when (storageType.lowercase()) {
            "pvc" -> File("$pvcPath/$exportId.${format.name}")

            "s3" -> File("/tmp/audit-export-$exportId.${format.name}")

            // загружается в S3 после
            else -> File("/tmp/audit-export-$exportId.${format.name}")
        }

    private fun resolveDownloadUrl(
        exportId: String,
        format: ExportFormat,
    ): String =
        when (storageType.lowercase()) {
            "pvc" -> "/api/v1/audit/export/$exportId/download"
            else -> "/api/v1/audit/export/$exportId/download"
        }
}
