package com.echomessenger.audit.unit

import com.echomessenger.audit.domain.CursorCodec
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class CursorCodecTest {
    @Test
    fun `encode and decode roundtrip`() {
        val ts = 1736760000000L
        val id = "550e8400-e29b-41d4-a716-446655440000"

        val encoded = CursorCodec.encode(ts, id)
        val (decodedTs, decodedId) = CursorCodec.decode(encoded)

        assertEquals(ts, decodedTs)
        assertEquals(id, decodedId)
    }

    @Test
    fun `encoded cursor is URL-safe base64`() {
        val encoded = CursorCodec.encode(1736760000000L, "abc-123")
        // URL-safe base64 не должен содержать + или /
        assertFalse(encoded.contains('+'), "Cursor must not contain '+'")
        assertFalse(encoded.contains('/'), "Cursor must not contain '/'")
        assertFalse(encoded.contains('='), "Cursor must not contain padding '='")
    }

    @Test
    fun `decode throws on invalid cursor`() {
        assertThrows(IllegalArgumentException::class.java) {
            CursorCodec.decode("aW52YWxpZA") // base64 "invalid" без разделителя
        }
    }

    @Test
    fun `encode with log_id containing colons`() {
        // log_id может содержать двоеточия в некоторых форматах
        val ts = 1000L
        val id = "prefix:suffix:extra"

        val encoded = CursorCodec.encode(ts, id)
        val (decodedTs, decodedId) = CursorCodec.decode(encoded)

        assertEquals(ts, decodedTs)
        assertEquals(id, decodedId)
    }

    @Test
    fun `different inputs produce different cursors`() {
        val cursor1 = CursorCodec.encode(1000L, "id1")
        val cursor2 = CursorCodec.encode(2000L, "id1")
        val cursor3 = CursorCodec.encode(1000L, "id2")

        assertNotEquals(cursor1, cursor2)
        assertNotEquals(cursor1, cursor3)
        assertNotEquals(cursor2, cursor3)
    }

    @ParameterizedTest
    @ValueSource(longs = [0L, 1L, Long.MAX_VALUE / 2, 1736760000000L])
    fun `encode and decode with various timestamps`(ts: Long) {
        val encoded = CursorCodec.encode(ts, "test-id")
        val (decodedTs, _) = CursorCodec.decode(encoded)
        assertEquals(ts, decodedTs)
    }
}
