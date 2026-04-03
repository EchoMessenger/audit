package com.echomessenger.audit.unit

import com.echomessenger.audit.domain.MessageReportItem
import com.echomessenger.audit.service.UserNameResolver
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

/**
 * HTTP-резолвер имён через restauth: кэш, negative cache на 404, устойчивость к 5xx.
 * Поднимает [MockWebServer] локально, без Spring-контекста.
 */
class UserNameResolverTest {
    private lateinit var server: MockWebServer

    @BeforeEach
    fun setup() {
        server = MockWebServer()
        server.start()
    }

    @AfterEach
    fun tearDown() {
        server.shutdown()
    }

    private fun resolver(
        baseUrl: String = server.url("/").toString().trimEnd('/'),
        cacheTtlSeconds: Long = 3600,
        cacheSize: Long = 500,
        negativeCacheTtlSeconds: Long = 120,
    ) = UserNameResolver(
        restauthBaseUrl = baseUrl,
        cacheTtlSeconds = cacheTtlSeconds,
        cacheSize = cacheSize,
        negativeCacheTtlSeconds = negativeCacheTtlSeconds,
    )

    @Test
    fun `resolveDisplayName hits restauth once then uses positive cache`() {
        server.enqueue(
            MockResponse()
                .setBody("""{"tinodeUid":"u1","displayName":"Alice"}""")
                .addHeader("Content-Type", "application/json"),
        )
        server.enqueue(
            MockResponse()
                .setBody("""{"displayName":"Stale"}""")
                .addHeader("Content-Type", "application/json"),
        )

        val r = resolver()
        assertEquals("Alice", r.resolveDisplayName("u1"))
        assertEquals("Alice", r.resolveDisplayName("u1"))
        assertEquals(1, server.requestCount)
    }

    @Test
    fun `resolveDisplayName returns null when base URL is blank`() {
        val r = resolver(baseUrl = "   ")
        assertNull(r.resolveDisplayName("any"))
        assertEquals(0, server.requestCount)
    }

    @Test
    fun `resolveDisplayName negative-caches 404`() {
        server.enqueue(MockResponse().setResponseCode(404).setBody("""{"err":"not found"}"""))
        server.enqueue(
            MockResponse()
                .setBody("""{"displayName":"New"}""")
                .addHeader("Content-Type", "application/json"),
        )

        val r = resolver()
        assertNull(r.resolveDisplayName("missing"))
        assertNull(r.resolveDisplayName("missing"))
        assertEquals(1, server.requestCount)
    }

    @Test
    fun `resolveDisplayName does not negative-cache server error — retries on next call`() {
        server.enqueue(MockResponse().setResponseCode(500))
        server.enqueue(
            MockResponse()
                .setBody("""{"displayName":"OkAfterFail"}""")
                .addHeader("Content-Type", "application/json"),
        )

        val r = resolver()
        assertNull(r.resolveDisplayName("u1"))
        assertEquals("OkAfterFail", r.resolveDisplayName("u1"))
        assertEquals(2, server.requestCount)
    }

    @Test
    fun `resolveDisplayName treats empty displayName as miss and negative-caches`() {
        server.enqueue(
            MockResponse()
                .setBody("""{"tinodeUid":"u1","displayName":""}""")
                .addHeader("Content-Type", "application/json"),
        )
        server.enqueue(
            MockResponse()
                .setBody("""{"displayName":"Later"}""")
                .addHeader("Content-Type", "application/json"),
        )

        val r = resolver()
        assertNull(r.resolveDisplayName("u1"))
        assertNull(r.resolveDisplayName("u1"))
        assertEquals(1, server.requestCount)
    }

    @Test
    fun `enrichMissingUserNames fills userName for same uid once`() {
        server.enqueue(
            MockResponse()
                .setBody("""{"displayName":"Bob"}""")
                .addHeader("Content-Type", "application/json"),
        )

        val r = resolver()
        val items =
            listOf(
                MessageReportItem(
                    messageId = 1L,
                    topicId = "t",
                    userId = "uid-bob",
                    userName = null,
                    timestamp = 1L,
                    content = "a",
                    isDeleted = false,
                ),
                MessageReportItem(
                    messageId = 2L,
                    topicId = "t",
                    userId = "uid-bob",
                    userName = "",
                    timestamp = 2L,
                    content = "b",
                    isDeleted = false,
                ),
            )

        val out = r.enrichMissingUserNames(items)
        assertEquals("Bob", out[0].userName)
        assertEquals("Bob", out[1].userName)
        assertEquals(1, server.requestCount)
    }

    @Test
    fun `enrichMissingUserNames skips rows that already have userName`() {
        val r = resolver()
        val items =
            listOf(
                MessageReportItem(
                    messageId = 1L,
                    topicId = "t",
                    userId = "uid",
                    userName = "KeepMe",
                    timestamp = 1L,
                    content = "c",
                    isDeleted = false,
                ),
            )
        assertEquals("KeepMe", r.enrichMissingUserNames(items).single().userName)
        assertEquals(0, server.requestCount)
    }

    @Test
    fun `resolveDisplayName strips tinode uid whitespace before request`() {
        server.enqueue(
            MockResponse()
                .setBody("""{"displayName":"Zed"}""")
                .addHeader("Content-Type", "application/json"),
        )
        val r = resolver()
        assertEquals("Zed", r.resolveDisplayName("  z-1  "))
        val path = server.takeRequest().path!!
        assertTrue(path.contains("z-1"), "path was $path")
        assertTrue(!path.contains(" "), "path should not contain raw spaces")
    }
}
