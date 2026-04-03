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
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.services.s3.presigner.S3Presigner
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest
import java.time.Duration
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
    @Value("\${audit.export.s3.bucket:audit-exports}") private val s3Bucket: String = "audit-exports",
    @Value("\${audit.export.s3.key-prefix:audit/exports}") private val s3KeyPrefix: String = "audit/exports",
    @Value("\${audit.export.s3.presign-expiry-seconds:900}") private val s3PresignExpirySeconds: Long = 900,
    private val s3Client: S3Client? = null,
    private val s3Presigner: S3Presigner? = null,
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
        val objectKey = buildS3ObjectKey(job.exportId, job.format)

        try {
            val messageReportReq = request.filters.toMessageReportRequest()

            BufferedWriter(FileWriter(outputFile)).use { writer ->
                when (job.format) {
                    ExportFormat.csv -> exportCsv(writer, messageReportReq, job.exportId)
                    ExportFormat.json -> exportJson(writer, messageReportReq, job.exportId)
                }
            }

            if (isS3Storage()) {
                uploadToS3(outputFile, objectKey, job.format)
            }

            val downloadUrl = resolveDownloadUrl(job.exportId)
            val fileSizeBytes = outputFile.length()
            exportRepository.save(
                job.copy(
                    status = ExportStatus.completed,
                    completedAt = System.currentTimeMillis(),
                    downloadUrl = downloadUrl,
                    fileSizeBytes = fileSizeBytes,
                ),
            )
            log.info("Export completed exportId={} bytes={} storageType={}", job.exportId, fileSizeBytes, storageType)
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
        } finally {
            if (isS3Storage() && outputFile.exists()) {
                outputFile.delete()
            }
        }
    }

    fun isS3Storage(): Boolean = storageType.equals("s3", ignoreCase = true)

    fun hasS3Object(
        exportId: String,
        format: ExportFormat,
    ): Boolean {
        if (!isS3Storage()) return false
        val s3 = requireNotNull(s3Client) { "S3 client is not configured while storageType=s3" }
        val key = buildS3ObjectKey(exportId, format)

        return try {
            s3.headObject(
                HeadObjectRequest.builder()
                    .bucket(s3Bucket)
                    .key(key)
                    .build(),
            )
            true
        } catch (e: S3Exception) {
            if (e.statusCode() == 404) {
                false
            } else {
                throw e
            }
        }
    }

    @Suppress("UNUSED_PARAMETER")
    fun generatePresignedDownloadUrl(
        exportId: String,
        format: ExportFormat,
    ): String {
        require(isS3Storage()) { "Presigned URL is available only for storageType=s3" }
        val presigner = requireNotNull(s3Presigner) { "S3 presigner is not configured while storageType=s3" }
        val key = buildS3ObjectKey(exportId, format)

        val getObjectRequest =
            GetObjectRequest.builder()
                .bucket(s3Bucket)
                .key(key)
                .build()

        val presignRequest =
            GetObjectPresignRequest.builder()
                .signatureDuration(Duration.ofSeconds(s3PresignExpirySeconds.coerceAtLeast(60L)))
                .getObjectRequest(getObjectRequest)
                .build()

        return presigner.presignGetObject(presignRequest).url().toString()
    }

    private fun exportCsv(
        writer: BufferedWriter,
        req: MessageReportRequest,
        exportId: String,
    ) {
        // Header
        writer.write("message_id,topic_id,user_id,user_name,timestamp,content,is_deleted")
        writer.newLine()

        var batches = 0
        var totalMessages = 0
        var repositoryResolved = 0
        var resolverRecovered = 0
        var unresolvedAfterResolver = 0

        messageRepository.streamMessages(req, batchSize = 1000) { batch ->
            batches += 1
            totalMessages += batch.size
            repositoryResolved += batch.count { !it.userName.isNullOrBlank() }
            val missingBeforeResolver = batch.count { it.userName.isNullOrBlank() }

            val enriched = userNameResolver.enrichMissingUserNames(batch)
            val missingAfterResolver = enriched.count { it.userName.isNullOrBlank() }
            resolverRecovered += (missingBeforeResolver - missingAfterResolver)
            unresolvedAfterResolver += missingAfterResolver

            log.info(
                "CSV export batch exportId={} batch={} size={} repositoryResolved={} recoveredByResolver={} unresolvedAfterResolver={}",
                exportId,
                batches,
                batch.size,
                batch.size - missingBeforeResolver,
                missingBeforeResolver - missingAfterResolver,
                missingAfterResolver,
            )

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

        log.info(
            "CSV export enrichment summary exportId={} batches={} totalMessages={} repositoryResolved={} recoveredByResolver={} unresolvedAfterResolver={}",
            exportId,
            batches,
            totalMessages,
            repositoryResolved,
            resolverRecovered,
            unresolvedAfterResolver,
        )
    }

    private fun exportJson(
        writer: BufferedWriter,
        req: MessageReportRequest,
        exportId: String,
    ) {
        writer.write("[")
        var first = true

        var batches = 0
        var totalMessages = 0
        var repositoryResolved = 0
        var resolverRecovered = 0
        var unresolvedAfterResolver = 0

        messageRepository.streamMessages(req, batchSize = 1000) { batch ->
            batches += 1
            totalMessages += batch.size
            repositoryResolved += batch.count { !it.userName.isNullOrBlank() }
            val missingBeforeResolver = batch.count { it.userName.isNullOrBlank() }

            val enriched = userNameResolver.enrichMissingUserNames(batch)
            val missingAfterResolver = enriched.count { it.userName.isNullOrBlank() }
            resolverRecovered += (missingBeforeResolver - missingAfterResolver)
            unresolvedAfterResolver += missingAfterResolver

            log.info(
                "JSON export batch exportId={} batch={} size={} repositoryResolved={} recoveredByResolver={} unresolvedAfterResolver={}",
                exportId,
                batches,
                batch.size,
                batch.size - missingBeforeResolver,
                missingBeforeResolver - missingAfterResolver,
                missingAfterResolver,
            )

            enriched.forEach { msg ->
                if (!first) writer.write(",")
                writer.newLine()
                writer.write(mapper.writeValueAsString(msg))
                first = false
            }
        }

        writer.newLine()
        writer.write("]")

        log.info(
            "JSON export enrichment summary exportId={} batches={} totalMessages={} repositoryResolved={} recoveredByResolver={} unresolvedAfterResolver={}",
            exportId,
            batches,
            totalMessages,
            repositoryResolved,
            resolverRecovered,
            unresolvedAfterResolver,
        )
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

    private fun resolveDownloadUrl(exportId: String): String =
        when (storageType.lowercase()) {
            "pvc" -> "/api/v1/audit/export/$exportId/download"
            else -> "/api/v1/audit/export/$exportId/download"
        }

    private fun uploadToS3(
        outputFile: File,
        objectKey: String,
        format: ExportFormat,
    ) {
        val s3 = requireNotNull(s3Client) { "S3 client is not configured while storageType=s3" }

        s3.putObject(
            PutObjectRequest.builder()
                .bucket(s3Bucket)
                .key(objectKey)
                .contentType(resolveContentType(format))
                .build(),
            RequestBody.fromFile(outputFile),
        )
    }

    private fun resolveContentType(format: ExportFormat): String =
        when (format) {
            ExportFormat.csv -> "text/csv"
            ExportFormat.json -> "application/json"
        }

    private fun buildS3ObjectKey(
        exportId: String,
        format: ExportFormat,
    ): String {
        val prefix = s3KeyPrefix.trim().trim('/')
        val fileName = "$exportId.${format.name}"
        return if (prefix.isBlank()) fileName else "$prefix/$fileName"
    }
}
