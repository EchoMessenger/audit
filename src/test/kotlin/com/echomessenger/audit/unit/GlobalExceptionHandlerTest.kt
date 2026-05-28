package com.echomessenger.audit.unit

import com.echomessenger.audit.exception.GlobalExceptionHandler
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.http.HttpStatus
import org.springframework.security.access.AccessDeniedException
import org.springframework.web.bind.MissingServletRequestParameterException
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException

class GlobalExceptionHandlerTest {
    private val handler = GlobalExceptionHandler()

    @Test
    fun `handles bad request errors with public message`() {
        val illegal = handler.handleIllegalArgument(IllegalArgumentException("bad filter"))
        assertEquals(HttpStatus.BAD_REQUEST, illegal.statusCode)
        assertEquals("bad filter", illegal.body?.message)

        val missing = handler.handleMissingParameter(MissingServletRequestParameterException("fromTs", "Long"))
        assertEquals(HttpStatus.BAD_REQUEST, missing.statusCode)
        assertEquals("Missing required parameter 'fromTs'", missing.body?.message)

        val mismatch = mockk<MethodArgumentTypeMismatchException>()
        every { mismatch.name } returns "limit"
        every { mismatch.message } returns "not an int"
        val typeMismatch = handler.handleTypeMismatch(mismatch)
        assertEquals(HttpStatus.BAD_REQUEST, typeMismatch.statusCode)
        assertEquals("Invalid parameter 'limit': not an int", typeMismatch.body?.message)
    }

    @Test
    fun `handles not found forbidden and generic errors`() {
        val notFound = handler.handleNotFound(NoSuchElementException("missing"))
        assertEquals(HttpStatus.NOT_FOUND, notFound.statusCode)
        assertEquals("missing", notFound.body?.message)

        val forbidden = handler.handleForbidden(AccessDeniedException("raw details"))
        assertEquals(HttpStatus.FORBIDDEN, forbidden.statusCode)
        assertEquals("Insufficient permissions", forbidden.body?.message)

        val generic = handler.handleGeneric(RuntimeException("secret details"))
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, generic.statusCode)
        assertEquals("An unexpected error occurred", generic.body?.message)
    }
}
