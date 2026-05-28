package com.echomessenger.audit.unit

import com.echomessenger.audit.support.AuditEventMapping
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

class AuditEventMappingTest {
    @Test
    fun `maps DEL variants to canonical event types`() {
        val cases =
            mapOf(
                "MSG" to "message.delete",
                "TOPIC" to "topic.delete",
                "SUB" to "subscription.leave",
                "USER" to "account.delete",
                "CRED" to "credential.delete",
                "" to "message.delete",
            )

        cases.forEach { (delWhat, expected) ->
            assertEquals(
                expected,
                AuditEventMapping.mapMsgTypeToEventType("DEL", null, null, null, delWhat),
            )
        }
        assertEquals("message.delete", AuditEventMapping.mapMsgTypeToEventType("DEL", null, null, null, null))
        assertEquals("unknown.DEL", AuditEventMapping.mapMsgTypeToEventType("DEL", null, null, null, "OTHER"))
    }

    @Test
    fun `maps GET variants to read event types`() {
        val cases =
            mapOf(
                "sub" to "subscription.read",
                "desc" to "topic.read",
                "data tags" to "topic.read",
                "cred" to "credential.read",
                "" to "topic.read",
                "unknown" to "topic.read",
            )

        cases.forEach { (getWhat, expected) ->
            assertEquals(
                expected,
                AuditEventMapping.mapMsgTypeToEventType("GET", null, getWhat, null, null),
            )
        }
    }

    @Test
    fun `maps SET and subscription database actions`() {
        assertEquals("topic.update", AuditEventMapping.mapMsgTypeToEventType("SET", null, null, "grpTopic", null))
        assertEquals("account.update", AuditEventMapping.mapMsgTypeToEventType("SET", null, null, null, null))

        assertEquals("subscription.join", AuditEventMapping.subscriptionDbActionToEventType("CREATE"))
        assertEquals("subscription.role_change", AuditEventMapping.subscriptionDbActionToEventType("UPDATE"))
        assertEquals("subscription.leave", AuditEventMapping.subscriptionDbActionToEventType("DELETE"))
        assertEquals("subscription.custom", AuditEventMapping.subscriptionDbActionToEventType("CUSTOM"))
    }

    @Test
    fun `maps all simple and reverse event types`() {
        val cases =
            mapOf(
                "LOGIN" to "auth.login",
                "HI" to "auth.session_start",
                "BYE" to "auth.logout",
                "REG" to "auth.register",
                "PUB" to "message.create",
                "EDIT" to "message.edit",
                "DEL" to "message.delete",
                "HDEL" to "message.hard_delete",
                "CREATE" to "topic.create",
                "DELETE" to "topic.delete",
                "JOIN" to "subscription.join",
                "LEAVE" to "subscription.leave",
                "ROLE" to "subscription.role_change",
            )

        cases.forEach { (msgType, eventType) ->
            assertEquals(eventType, AuditEventMapping.mapSimpleMsgTypeToEventType(msgType))
            assertEquals(listOf(msgType), AuditEventMapping.mapEventTypeToMsgTypes(eventType))
        }
        assertEquals("unknown.CUSTOM", AuditEventMapping.mapSimpleMsgTypeToEventType("CUSTOM"))
        assertNull(AuditEventMapping.mapEventTypeToMsgTypes("unknown.custom"))
    }
}
