package com.echomessenger.audit.support

import com.echomessenger.audit.domain.AuditEvent
import java.util.UUID

/**
 * Согласует порядок с keyset в ClickHouse: ORDER BY log_timestamp DESC, log_id DESC.
 */
object AuditEventSort {
    val DESC: Comparator<AuditEvent> =
        Comparator { left, right ->
            val byTimestamp = right.timestamp.compareTo(left.timestamp)
            if (byTimestamp != 0) {
                byTimestamp
            } else {
                compareEventIdsDesc(left.eventId, right.eventId)
            }
        }

    private fun compareEventIdsDesc(
        leftRaw: String,
        rightRaw: String,
    ): Int {
        val leftUuid = safeUuid(leftRaw)
        val rightUuid = safeUuid(rightRaw)

        return when {
            // ClickHouse compares UUID as UInt128 with least-significant bits first.
            // Keep the same order in-memory, otherwise cursor pagination may duplicate rows.
            leftUuid != null && rightUuid != null -> compareUuidLsbFirst(rightUuid, leftUuid)
            else -> rightRaw.compareTo(leftRaw)
        }
    }

    private fun compareUuidLsbFirst(
        left: UUID,
        right: UUID,
    ): Int {
        val lsb = java.lang.Long.compare(left.leastSignificantBits, right.leastSignificantBits)
        if (lsb != 0) return lsb
        return java.lang.Long.compare(left.mostSignificantBits, right.mostSignificantBits)
    }

    private fun safeUuid(raw: String): UUID? = runCatching { UUID.fromString(raw) }.getOrNull()
}
