package com.echomessenger.audit.support

import com.echomessenger.audit.domain.AuditEvent
import java.util.UUID

/**
 * Согласует порядок с keyset в ClickHouse: ORDER BY log_timestamp DESC, log_id DESC.
 */
object AuditEventSort {
    val DESC: Comparator<AuditEvent> =
        compareByDescending<AuditEvent> { it.timestamp }
            .thenByDescending { uuidFromEventId(it.eventId) }

    private fun uuidFromEventId(raw: String): UUID = UUID.fromString(raw)
}
