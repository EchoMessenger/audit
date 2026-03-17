package com.echomessenger.audit.service

import com.echomessenger.audit.domain.AuditEvent
import com.echomessenger.audit.domain.CursorPage
import com.echomessenger.audit.repository.AuditRepository
import com.echomessenger.audit.repository.SessionRepository
import com.echomessenger.audit.repository.UserTimelineRepository
import org.springframework.stereotype.Service

@Service
class AuditService(
    private val auditRepository: AuditRepository,
    private val sessionRepository: SessionRepository,
    private val userTimelineRepository: UserTimelineRepository,
) {
    fun findEvents(
        userId: String?,
        actorUserId: String?,
        topicId: String?,
        eventType: String?,
        fromTs: Long?,
        toTs: Long?,
        status: String?,
        cursor: String?,
        limit: Int,
    ): CursorPage<AuditEvent> =
        auditRepository.findEvents(
            userId = userId,
            actorUserId = actorUserId,
            topicId = topicId,
            eventType = eventType,
            fromTs = fromTs,
            toTs = toTs,
            status = status,
            cursor = cursor,
            limit = limit,
        )

    fun findEventById(eventId: String): AuditEvent? = auditRepository.findEventById(eventId)

    fun findAuthEvents(
        userId: String?,
        fromTs: Long?,
        toTs: Long?,
        cursor: String?,
        limit: Int,
    ): CursorPage<AuditEvent> =
        auditRepository.findAuthEvents(
            userId = userId,
            fromTs = fromTs,
            toTs = toTs,
            cursor = cursor,
            limit = limit,
        )

    fun findSessions(
        userId: String?,
        fromTs: Long?,
        toTs: Long?,
        cursor: String?,
        limit: Int,
    ) = sessionRepository.findSessions(
        userId = userId,
        fromTs = fromTs,
        toTs = toTs,
        cursor = cursor,
        limit = limit,
    )

    fun getUserTimeline(
        userId: String,
        fromTs: Long?,
        toTs: Long?,
        cursor: String?,
        limit: Int,
    ) = userTimelineRepository.getTimeline(
        userId = userId,
        fromTs = fromTs,
        toTs = toTs,
        cursor = cursor,
        limit = limit,
    )
}
