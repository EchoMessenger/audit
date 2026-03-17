package com.echomessenger.audit.domain

import com.fasterxml.jackson.annotation.JsonInclude
import java.time.Instant
import java.util.Base64

// ── AuditEvent ─────────────────────────────────────────────────────────────

/**
 * Унифицированная запись аудита. Маппинг из разных ClickHouse таблиц
 * через event_type как дискриминатор.
 *
 * Маппинг msg_type → event_type:
 *   client_req_log.msg_type=LOGIN   → auth.login
 *   client_req_log.msg_type=HI      → auth.session_start
 *   client_req_log.msg_type=BYE     → auth.logout
 *   client_req_log.msg_type=REG     → auth.register
 *   message_log.msg_type=PUB        → message.create
 *   message_log.msg_type=EDIT       → message.edit
 *   message_log.msg_type=DEL        → message.delete
 *   topic_log.action=CREATE         → topic.create
 *   topic_log.action=DELETE         → topic.delete
 *   account_log.action=UPDATE       → account.update
 *   subscription_log.action=JOIN    → subscription.join
 *   subscription_log.action=LEAVE   → subscription.leave
 *   subscription_log.action=ROLE    → subscription.role_change
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
data class AuditEvent(
    val eventId: String,
    val eventType: String,
    val timestamp: Long, // UNIX ms
    val userId: String?,
    val actorUserId: String?,
    val topicId: String?,
    val status: String, // "success" | "failure"
    val metadata: Map<String, Any?> = emptyMap(),
    // auth-specific fields (только в auth-events ответах)
    val ip: String? = null,
    val userAgent: String? = null,
    val deviceId: String? = null,
)

// ── Cursor Pagination ───────────────────────────────────────────────────────

/**
 * Cursor кодируется как base64(timestamp_ms:log_id).
 * Стабильно при вставке новых данных — в отличие от offset.
 */
data class CursorPage<T>(
    val data: List<T>,
    val nextCursor: String?,
    val hasMore: Boolean,
)

object CursorCodec {
    fun encode(
        timestampMs: Long,
        logId: String,
    ): String =
        Base64
            .getUrlEncoder()
            .withoutPadding()
            .encodeToString("$timestampMs:$logId".toByteArray())

    fun decode(cursor: String): Pair<Long, String> {
        val raw = String(Base64.getUrlDecoder().decode(cursor))
        val idx = raw.indexOf(':')
        require(idx > 0) { "Invalid cursor format" }
        return raw.substring(0, idx).toLong() to raw.substring(idx + 1)
    }
}

// ── Session ─────────────────────────────────────────────────────────────────

data class SessionSummary(
    val sessionId: String,
    val userId: String?,
    val firstEventAt: Long,
    val lastEventAt: Long,
    val durationSeconds: Long,
    val eventCount: Long,
    val ipAddresses: List<String>,
)

// ── Incident ────────────────────────────────────────────────────────────────

data class Incident(
    val incidentId: String,
    val type: String,
    val status: String, // "open" | "confirmed" | "dismissed"
    val detectedAt: Long,
    val userId: String?,
    val details: Map<String, Any?> = emptyMap(),
    val updatedAt: Long,
)

data class IncidentDetail(
    val incident: Incident,
    val events: List<AuditEvent>,
)

data class UpdateIncidentStatusRequest(
    val status: String,
    val comment: String? = null,
)

// ── ExportJob ───────────────────────────────────────────────────────────────

enum class ExportFormat { csv, json }

enum class ExportStatus { pending, running, completed, failed }

data class ExportJob(
    val exportId: String,
    val status: ExportStatus,
    val format: ExportFormat,
    val createdAt: Long,
    val completedAt: Long?,
    val downloadUrl: String?,
    val errorMessage: String?,
    val fileSizeBytes: Long?,
)

data class ExportRequest(
    val filters: ExportFilters,
    val format: ExportFormat = ExportFormat.csv,
)

data class ExportFilters(
    val userId: String? = null,
    val topicId: String? = null,
    val eventType: String? = null,
    val fromTs: Long? = null,
    val toTs: Long? = null,
    val status: String? = null,
)

// ── RetentionPolicy ─────────────────────────────────────────────────────────

data class RetentionPolicy(
    val tableName: String,
    val database: String,
    val engine: String,
    val ttlExpression: String?,
    val retentionDays: Int?,
)

// ── Report ──────────────────────────────────────────────────────────────────

data class MessageReportRequest(
    val users: List<String>? = null,
    val topics: List<String>? = null,
    val fromTs: Long,
    val toTs: Long,
    val includeDeleted: Boolean = false,
)

data class MessageReportItem(
    val messageId: Long,
    val topicId: String,
    val userId: String,
    val userName: String?,
    val timestamp: Long,
    val content: String?,
    val isDeleted: Boolean,
)

data class MessageReport(
    val reportId: String,
    val generatedAt: Long,
    val totalMessages: Long,
    val messages: List<MessageReportItem>,
)
