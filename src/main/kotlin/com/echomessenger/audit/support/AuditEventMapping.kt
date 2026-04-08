package com.echomessenger.audit.support

import com.echomessenger.audit.domain.AuditEvent
import java.sql.ResultSet

object AuditEventMapping {
    fun mapAuditEventFromResultSet(rs: ResultSet): AuditEvent {
        val msgType = rs.getString("msg_type") ?: ""
        val userId = rs.getString("user_id")?.takeIf { it.isNotBlank() }
        val subTopic = rs.getString("sub_topic")?.takeIf { it.isNotBlank() }
        val getWhat = rs.getString("get_what")?.takeIf { it.isNotBlank() }
        val setTopic = rs.getString("set_topic")?.takeIf { it.isNotBlank() }
        val delWhat = rs.getString("del_what")?.takeIf { it.isNotBlank() }
        val delUserId = rs.getString("del_user_id")?.takeIf { it.isNotBlank() }

        return AuditEvent(
            eventId = rs.getString("event_id"),
            eventType = mapMsgTypeToEventType(msgType, subTopic, getWhat, setTopic, delWhat),
            timestamp = rs.getLong("timestamp"),
            userId = userId,
            actorUserId = userId,
            topicId = rs.getString("topic_id")?.takeIf { it.isNotBlank() },
            status = rs.getString("status"),
            metadata =
                buildMap {
                    if (msgType.isNotBlank()) put("msg_type", msgType)
                    subTopic?.let { put("sub_topic", it) }
                    getWhat?.let { put("get_what", it) }
                    setTopic?.let { put("set_topic", it) }
                    delWhat?.let { put("del_what", it) }
                    delUserId?.let { put("del_user_id", it) }
                    rs
                        .getString("sess_session_id")
                        ?.takeIf { it.isNotBlank() }
                        ?.let { put("session_id", it) }
                },
            ip = rs.getString("ip")?.takeIf { it.isNotBlank() },
            userAgent = rs.getString("user_agent")?.takeIf { it.isNotBlank() },
            deviceId = rs.getString("device_id")?.takeIf { it.isNotBlank() },
        )
    }

    fun mapMsgTypeToEventType(
        msgType: String,
        subTopic: String?,
        getWhat: String?,
        setTopic: String?,
        delWhat: String?,
    ): String {
        val msg = msgType.uppercase()
        return when (msg) {
            "SUB" -> {
                val topic = subTopic?.trim()?.lowercase().orEmpty()
                when {
                    topic == "new" -> "topic.create"
                    topic == "me" -> "subscription.me"
                    topic == "fnd" -> "search.query"
                    topic.startsWith("grp") -> "subscription.join"
                    topic.startsWith("usr") -> "subscription.join"
                    topic.isBlank() -> "subscription.update"
                    else -> "subscription.update"
                }
            }

            "DEL" -> {
                when (delWhat?.trim()?.uppercase()) {
                    "MSG" -> "message.delete"
                    "TOPIC" -> "topic.delete"
                    "SUB" -> "subscription.leave"
                    "USER" -> "account.delete"
                    "CRED" -> "credential.delete"
                    null, "" -> "message.delete"
                    else -> "unknown.$msgType"
                }
            }

            "GET" -> {
                val what = getWhat?.trim()?.lowercase().orEmpty()
                when {
                    what.contains("sub") -> "subscription.read"
                    what.contains("desc") || what.contains("data") || what.contains("tags") -> "topic.read"
                    what.contains("cred") -> "credential.read"
                    what.isBlank() -> "topic.read"
                    else -> "topic.read"
                }
            }

            "SET" -> {
                if (!setTopic.isNullOrBlank()) "topic.update" else "account.update"
            }

            else -> mapSimpleMsgTypeToEventType(msgType)
        }
    }

    fun mapSimpleMsgTypeToEventType(msgType: String): String =
        when (msgType.uppercase()) {
            "LOGIN" -> "auth.login"
            "HI" -> "auth.session_start"
            "BYE" -> "auth.logout"
            "REG" -> "auth.register"
            "PUB" -> "message.create"
            "EDIT" -> "message.edit"
            "DEL" -> "message.delete"
            "HDEL" -> "message.hard_delete"
            "CREATE" -> "topic.create"
            "DELETE" -> "topic.delete"
            "JOIN" -> "subscription.join"
            "LEAVE" -> "subscription.leave"
            "ROLE" -> "subscription.role_change"
            else -> "unknown.$msgType"
        }

    /** Соответствие Enum8 subscription_log (CREATE/UPDATE/DELETE) каноническим event_type. */
    fun subscriptionDbActionToEventType(action: String): String =
        when (action.uppercase()) {
            "CREATE" -> "subscription.join"
            "UPDATE" -> "subscription.role_change"
            "DELETE" -> "subscription.leave"
            else -> "subscription.${action.lowercase()}"
        }

    fun mapEventTypeToMsgTypes(eventType: String): List<String>? =
        when (eventType) {
            "auth.login" -> listOf("LOGIN")
            "auth.session_start" -> listOf("HI")
            "auth.logout" -> listOf("BYE")
            "auth.register" -> listOf("REG")
            "message.create" -> listOf("PUB")
            "message.edit" -> listOf("EDIT")
            "message.delete" -> listOf("DEL")
            "message.hard_delete" -> listOf("HDEL")
            "topic.create" -> listOf("CREATE")
            "topic.delete" -> listOf("DELETE")
            "subscription.join" -> listOf("JOIN")
            "subscription.leave" -> listOf("LEAVE")
            "subscription.role_change" -> listOf("ROLE")
            else -> null
        }
}
