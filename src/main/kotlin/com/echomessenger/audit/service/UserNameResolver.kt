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

    enum class UserLookupStatus {
        FOUND,
        NOT_FOUND,
        UNAVAILABLE,
    }

    data class UserLookupResult(
        val status: UserLookupStatus,
        val displayName: String? = null,
    )

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

    private val emptyDisplayCache =
        Caffeine
            .newBuilder()
            .maximumSize(cacheSize)
            .expireAfterWrite(negativeCacheTtlSeconds, TimeUnit.SECONDS)
            .build<String, Boolean>()

    private val unavailableCache =
        Caffeine
            .newBuilder()
            .maximumSize(cacheSize)
            .expireAfterWrite(negativeCacheTtlSeconds, TimeUnit.SECONDS)
            .build<String, Boolean>()

    fun resolveDisplayName(tinodeUid: String?): String? = lookupUser(tinodeUid).displayName

    fun lookupUser(tinodeUid: String?): UserLookupResult {
        val uid = tinodeUid?.trim().takeIf { !it.isNullOrEmpty() }
            ?: return UserLookupResult(UserLookupStatus.NOT_FOUND)

        positiveCache.getIfPresent(uid)?.let {
            return UserLookupResult(
                status = UserLookupStatus.FOUND,
                displayName = it,
            )
        }
        if (emptyDisplayCache.getIfPresent(uid) == true) {
            return UserLookupResult(UserLookupStatus.FOUND)
        }
        if (negativeCache.getIfPresent(uid) == true) {
            return UserLookupResult(UserLookupStatus.NOT_FOUND)
        }
        if (unavailableCache.getIfPresent(uid) == true) {
            return UserLookupResult(UserLookupStatus.UNAVAILABLE)
        }

        val client = restClient
        if (client == null) {
            log.warn("RESTAUTH_BASE_URL is not configured, cannot resolve user uid={}", uid)
            return UserLookupResult(UserLookupStatus.UNAVAILABLE)
        }

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
            } else {
                // Mapping may exist with empty display name, cache short-term to avoid repeated calls.
                emptyDisplayCache.put(uid, true)
            }

            UserLookupResult(
                status = UserLookupStatus.FOUND,
                displayName = display,
            )
        } catch (e: HttpClientErrorException) {
            if (e.statusCode == HttpStatus.NOT_FOUND) {
                negativeCache.put(uid, true)
                return UserLookupResult(UserLookupStatus.NOT_FOUND)
            }

            log.warn("User resolve failed (uid={} status={})", uid, e.statusCode.value())
            unavailableCache.put(uid, true)
            UserLookupResult(UserLookupStatus.UNAVAILABLE)
        } catch (e: Exception) {
            // graceful fallback: do not fail report/export when restauth is temporarily unavailable
            log.warn("User resolve failed (uid={})", uid, e)
            unavailableCache.put(uid, true)
            UserLookupResult(UserLookupStatus.UNAVAILABLE)
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

