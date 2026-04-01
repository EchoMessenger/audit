package com.echomessenger.audit.service

import com.github.benmanes.caffeine.cache.Caffeine
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpStatus
import org.springframework.http.client.SimpleClientHttpRequestFactory
import org.springframework.stereotype.Service
import org.springframework.web.client.HttpClientErrorException
import org.springframework.web.client.RestClient
import java.time.Duration
import java.util.concurrent.TimeUnit

@Service
class UserNameResolver(
    @Value("\${RESTAUTH_BASE_URL:}") private val restauthBaseUrl: String,
    @Value("\${USER_RESOLUTION_CACHE_TTL:3600}") private val cacheTtlSeconds: Long,
    @Value("\${USER_RESOLUTION_CACHE_SIZE:10000}") private val cacheSize: Long,
    @Value("\${USER_RESOLUTION_NEGATIVE_CACHE_TTL:120}") private val negativeCacheTtlSeconds: Long,
) {
    private val log = LoggerFactory.getLogger(UserNameResolver::class.java)

    private val restClient: RestClient? =
        restauthBaseUrl.trim().takeIf { it.isNotEmpty() }?.let { baseUrl ->
            val rf =
                SimpleClientHttpRequestFactory().apply {
                    setConnectTimeout(Duration.ofSeconds(2))
                    setReadTimeout(Duration.ofSeconds(3))
                }
            RestClient.builder().baseUrl(baseUrl.trimEnd('/')).requestFactory(rf).build()
        }

    private val positiveCache =
        Caffeine
            .newBuilder()
            .maximumSize(cacheSize)
            .expireAfterWrite(cacheTtlSeconds, TimeUnit.SECONDS)
            .build<String, String>()

    private val negativeCache =
        Caffeine
            .newBuilder()
            .maximumSize(cacheSize)
            .expireAfterWrite(negativeCacheTtlSeconds, TimeUnit.SECONDS)
            .build<String, Boolean>()

    fun resolveDisplayName(tinodeUid: String?): String? {
        val uid = tinodeUid?.trim().takeIf { !it.isNullOrEmpty() } ?: return null

        positiveCache.getIfPresent(uid)?.let { return it }
        if (negativeCache.getIfPresent(uid) == true) return null

        val client = restClient ?: return null

        return try {
            val resp =
                client
                    .get()
                    .uri("/users/by-tinode-uid/{uid}", uid)
                    .retrieve()
                    .body(RestauthUserResponse::class.java)

            val display = resp?.displayName?.trim().takeIf { !it.isNullOrEmpty() }
            if (display != null) {
                positiveCache.put(uid, display)
                return display
            }

            // If mapping exists but name is empty — treat as negative for a short time.
            negativeCache.put(uid, true)
            null
        } catch (e: HttpClientErrorException) {
            if (e.statusCode == HttpStatus.NOT_FOUND) {
                negativeCache.put(uid, true)
                return null
            }
            log.warn("User resolve failed (uid={} status={})", uid, e.statusCode.value())
            null
        } catch (e: Exception) {
            // graceful fallback: do not fail report/export
            log.warn("User resolve failed (uid={})", uid, e)
            null
        }
    }

    fun <T> enrichMissingUserNames(items: List<T>): List<T> where T : MessageWithUserName {
        if (items.isEmpty()) return items

        // Resolve per-unique uid; caching keeps it cheap.
        val missing =
            items
                .asSequence()
                .filter { it.userName.isNullOrBlank() }
                .mapNotNull { it.userId?.trim() }
                .filter { it.isNotEmpty() }
                .toSet()

        if (missing.isEmpty()) return items

        val resolved = missing.associateWith { resolveDisplayName(it) }

        return items.map { item ->
            if (!item.userName.isNullOrBlank()) return@map item
            val name = resolved[item.userId]
            @Suppress("UNCHECKED_CAST")
            if (name.isNullOrBlank()) item else item.withUserName(name) as T
        }
    }

    data class RestauthUserResponse(
        val tinodeUid: String? = null,
        val keycloakId: String? = null,
        val keycloakUsername: String? = null,
        val displayName: String? = null,
        val email: String? = null,
    )

    /**
     * Small adapter to reuse enrichment logic across different DTOs.
     */
    interface MessageWithUserName {
        /** Nullable — сообщение без автора в ClickHouse. */
        val userId: String?
        val userName: String?

        fun withUserName(userName: String): MessageWithUserName
    }
}

