package com.echomessenger.audit.integration

import com.echomessenger.audit.domain.*
import com.echomessenger.audit.repository.ExportRepository
import com.echomessenger.audit.repository.MessageRepository
import com.echomessenger.audit.service.ExportService
import com.echomessenger.audit.service.UserNameResolver
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.S3Configuration
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.HeadObjectRequest
import software.amazon.awssdk.services.s3.presigner.S3Presigner
import java.nio.file.Path
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.UUID
import kotlin.io.path.listDirectoryEntries

private fun chTs(instant: Instant): String =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
        .withZone(ZoneOffset.UTC)
        .format(instant)

class ExportServiceIT : IntegrationTestBase() {
    @Autowired private lateinit var exportRepository: ExportRepository
    @Autowired private lateinit var messageRepository: MessageRepository
    @Autowired private lateinit var jdbc: NamedParameterJdbcTemplate
    private val userNameResolver: UserNameResolver = io.mockk.mockk(relaxed = true)

    @TempDir lateinit var tempDir: Path

    private fun service() = ExportService(
        exportRepository = exportRepository,
        messageRepository = messageRepository,
        userNameResolver = userNameResolver,
        storageType = "pvc",
        pvcPath = tempDir.toString(),
    )

    @Test
    fun `CSV export creates file and reaches completed status`() {
        val userId = "exp_csv_${UUID.randomUUID().toString().take(8)}"
        seedMessages(userId)

        val job = service().startExport(
            ExportRequest(
                filters = ExportFilters(
                    userId = userId,
                    fromTs = System.currentTimeMillis() - 3_600_000,
                    toTs = System.currentTimeMillis(),
                ),
                format = ExportFormat.csv,
            ),
        )
        assertEquals(ExportStatus.pending, job.status)

        Thread.sleep(3000)

        val completed = (1..20).asSequence()
            .map { Thread.sleep(500); exportRepository.findById(job.exportId) }
            .firstOrNull { it?.status == ExportStatus.completed || it?.status == ExportStatus.failed }

        assertNotNull(completed)
        assertEquals(ExportStatus.completed, completed!!.status,
            "got: ${completed.status}, error: ${completed.errorMessage}")
        assertNotNull(completed.completedAt)
        assertNotNull(completed.downloadUrl)
        assertTrue((completed.fileSizeBytes ?: 0) > 0)

        val files = tempDir.listDirectoryEntries("${job.exportId}.csv")
        assertTrue(files.isNotEmpty())
        val content = files.first().toFile().readText()
        assertTrue(content.startsWith("message_id,"))
        assertTrue(content.lines().size > 1)
    }

    @Test
    fun `JSON export creates valid JSON array file`() {
        val userId = "exp_json_${UUID.randomUUID().toString().take(8)}"
        seedMessages(userId)

        val job = service().startExport(
            ExportRequest(
                filters = ExportFilters(
                    userId = userId,
                    fromTs = System.currentTimeMillis() - 3_600_000,
                    toTs = System.currentTimeMillis(),
                ),
                format = ExportFormat.json,
            ),
        )

        Thread.sleep(3000)

        val files = tempDir.listDirectoryEntries("${job.exportId}.json")
        assertTrue(files.isNotEmpty())
        val content = files.first().toFile().readText().trim()
        assertTrue(content.startsWith("["))
        assertTrue(content.endsWith("]"))
    }

    @Test
    fun `export with no matching data completes successfully with empty file`() {
        val job = service().startExport(
            ExportRequest(
                filters = ExportFilters(
                    userId = "nonexistent_${UUID.randomUUID()}",
                    fromTs = System.currentTimeMillis() - 3_600_000,
                    toTs = System.currentTimeMillis(),
                ),
                format = ExportFormat.csv,
            ),
        )

        Thread.sleep(3000)

        val completed = exportRepository.findById(job.exportId)
        assertEquals(ExportStatus.completed, completed?.status)
    }

    @Test
    fun `getJob returns null for unknown export id`() {
        assertNull(service().getJob("completely-unknown-id"))
    }

    @Test
    fun `getJob returns persisted job`() {
        val job = service().startExport(
            ExportRequest(
                filters = ExportFilters(
                    fromTs = System.currentTimeMillis() - 3600_000,
                    toTs = System.currentTimeMillis(),
                ),
                format = ExportFormat.csv,
            ),
        )
        val found = service().getJob(job.exportId)
        assertNotNull(found)
        assertEquals(job.exportId, found!!.exportId)
    }

    @Test
    fun `S3 export uploads object to MinIO and generates presigned URL`() {
        val minio =
            GenericContainer(DockerImageName.parse("minio/minio:latest"))
                .withExposedPorts(9000)
                .withEnv("MINIO_ROOT_USER", "minioadmin")
                .withEnv("MINIO_ROOT_PASSWORD", "minioadmin")
                .withCommand("server", "/data")
                .waitingFor(Wait.forHttp("/minio/health/ready").forStatusCode(200))

        minio.start()
        try {
            val endpoint = "http://${minio.host}:${minio.getMappedPort(9000)}"
            val bucket = "audit-exports-test"
            val keyPrefix = "it/exports"
            val credentialsProvider =
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create("minioadmin", "minioadmin"),
                )

            val s3Client =
                S3Client.builder()
                    .endpointOverride(java.net.URI.create(endpoint))
                    .region(Region.US_EAST_1)
                    .credentialsProvider(credentialsProvider)
                    .serviceConfiguration(
                        S3Configuration.builder()
                            .pathStyleAccessEnabled(true)
                            .build(),
                    ).build()

            val s3Presigner =
                S3Presigner.builder()
                    .endpointOverride(java.net.URI.create(endpoint))
                    .region(Region.US_EAST_1)
                    .credentialsProvider(credentialsProvider)
                    .build()

            s3Client.createBucket(CreateBucketRequest.builder().bucket(bucket).build())

            val s3Service =
                ExportService(
                    exportRepository = exportRepository,
                    messageRepository = messageRepository,
                    userNameResolver = userNameResolver,
                    storageType = "s3",
                    pvcPath = tempDir.toString(),
                    s3Bucket = bucket,
                    s3KeyPrefix = keyPrefix,
                    s3PresignExpirySeconds = 300,
                    s3Client = s3Client,
                    s3Presigner = s3Presigner,
                )

            val userId = "exp_s3_${UUID.randomUUID().toString().take(8)}"
            seedMessages(userId)

            val job =
                s3Service.startExport(
                    ExportRequest(
                        filters =
                            ExportFilters(
                                userId = userId,
                                fromTs = System.currentTimeMillis() - 3_600_000,
                                toTs = System.currentTimeMillis(),
                            ),
                        format = ExportFormat.csv,
                    ),
                )

            val completed = awaitTerminalStatus(job.exportId)
            assertEquals(ExportStatus.completed, completed.status, "error: ${completed.errorMessage}")
            assertTrue((completed.fileSizeBytes ?: 0) > 0)

            val objectKey = "$keyPrefix/${job.exportId}.csv"
            s3Client.headObject(
                HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(objectKey)
                    .build(),
            )

            val objectBody =
                s3Client
                    .getObjectAsBytes(
                        GetObjectRequest.builder().bucket(bucket).key(objectKey).build(),
                    ).asUtf8String()
            assertTrue(objectBody.startsWith("message_id,"))

            val presignedUrl = s3Service.generatePresignedDownloadUrl(job.exportId, ExportFormat.csv)
            assertTrue(presignedUrl.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"))
            assertTrue(presignedUrl.contains(job.exportId))
        } finally {
            minio.stop()
        }
    }

    private fun awaitTerminalStatus(exportId: String): ExportJob {
        repeat(40) {
            val current = exportRepository.findById(exportId)
            if (current != null && (current.status == ExportStatus.completed || current.status == ExportStatus.failed)) {
                return current
            }
            Thread.sleep(250)
        }

        return exportRepository.findById(exportId) ?: error("Export job was not found: $exportId")
    }

    private fun seedMessages(userId: String) {
        val now = Instant.now()
        listOf(
            Triple(1800L, "CREATE", 1),
            Triple(1200L, "CREATE", 2),
            Triple(600L,  "UPDATE", 1),
        ).forEach { (offsetSec, action, seqId) ->
            jdbc.update(
                """INSERT INTO audit.message_log
                   (log_id, log_timestamp, action, msg_topic, msg_from_user_id,
                    msg_timestamp, msg_seq_id, msg_content)
                   VALUES (:id, :ts, :act, :topic, :uid, :msgTs, :seqId, :content)""",
                MapSqlParameterSource()
                    .addValue("id", UUID.randomUUID().toString())
                    .addValue("ts", chTs(now.minusSeconds(offsetSec)))
                    .addValue("act", action)
                    .addValue("topic", "topic_exp")
                    .addValue("uid", userId)
                    .addValue("msgTs", now.minusSeconds(offsetSec).toEpochMilli())
                    .addValue("seqId", seqId)
                    .addValue("content", "Export msg $seqId"),
            )
        }
        Thread.sleep(500)
    }
}