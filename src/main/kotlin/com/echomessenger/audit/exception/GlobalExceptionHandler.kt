package com.echomessenger.audit.exception

import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.security.access.AccessDeniedException
import org.springframework.web.bind.MissingServletRequestParameterException
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException
import org.springframework.web.servlet.resource.NoResourceFoundException

data class ErrorResponse(
    val error: String,
    val message: String,
    val status: Int,
)

@RestControllerAdvice
class GlobalExceptionHandler {
    private val log = LoggerFactory.getLogger(GlobalExceptionHandler::class.java)
    private val forbiddenPublicMessage = "Insufficient permissions"

    @ExceptionHandler(IllegalArgumentException::class)
    fun handleIllegalArgument(e: IllegalArgumentException): ResponseEntity<ErrorResponse> =
        ResponseEntity
            .status(HttpStatus.BAD_REQUEST)
            .body(
                ErrorResponse(
                    error = "BAD_REQUEST",
                    message = e.message ?: "Invalid request parameters",
                    status = 400,
                ),
            )

    @ExceptionHandler(NoSuchElementException::class)
    fun handleNotFound(e: NoSuchElementException): ResponseEntity<ErrorResponse> =
        ResponseEntity
            .status(HttpStatus.NOT_FOUND)
            .body(
                ErrorResponse(
                    error = "NOT_FOUND",
                    message = e.message ?: "Resource not found",
                    status = 404,
                ),
            )

    @ExceptionHandler(AccessDeniedException::class)
    fun handleForbidden(e: AccessDeniedException): ResponseEntity<ErrorResponse> {
        log.warn("Access denied: {}", e.message, e)
        return ResponseEntity
            .status(HttpStatus.FORBIDDEN)
            .body(
                ErrorResponse(
                    error = "FORBIDDEN",
                    message = forbiddenPublicMessage,
                    status = 403,
                ),
            )
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException::class)
    fun handleTypeMismatch(e: MethodArgumentTypeMismatchException): ResponseEntity<ErrorResponse> =
        ResponseEntity
            .status(HttpStatus.BAD_REQUEST)
            .body(
                ErrorResponse(
                    error = "BAD_REQUEST",
                    message = "Invalid parameter '${e.name}': ${e.message}",
                    status = 400,
                ),
            )

    @ExceptionHandler(MissingServletRequestParameterException::class)
    fun handleMissingParameter(e: MissingServletRequestParameterException): ResponseEntity<ErrorResponse> =
        ResponseEntity
            .status(HttpStatus.BAD_REQUEST)
            .body(
                ErrorResponse(
                    error = "BAD_REQUEST",
                    message = "Missing required parameter '${e.parameterName}'",
                    status = 400,
                ),
            )

    @ExceptionHandler(NoResourceFoundException::class)
    fun handleNoResource(e: NoResourceFoundException): ResponseEntity<ErrorResponse> =
        ResponseEntity
            .status(HttpStatus.NOT_FOUND)
            .body(
                ErrorResponse(
                    error = "NOT_FOUND",
                    message = e.message ?: "Resource not found",
                    status = 404,
                ),
            )

    @ExceptionHandler(Exception::class)
    fun handleGeneric(e: Exception): ResponseEntity<ErrorResponse> {
        log.error("Unhandled exception", e)
        return ResponseEntity
            .status(HttpStatus.INTERNAL_SERVER_ERROR)
            .body(
                ErrorResponse(
                    error = "INTERNAL_ERROR",
                    message = "An unexpected error occurred",
                    status = 500,
                ),
            )
    }
}
