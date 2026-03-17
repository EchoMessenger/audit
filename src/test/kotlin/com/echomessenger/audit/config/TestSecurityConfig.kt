package com.echomessenger.audit.config

import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.security.oauth2.jwt.Jwt
import org.springframework.security.oauth2.jwt.JwtDecoder
import java.time.Instant

@TestConfiguration
class TestSecurityConfig {
    /**
     * @Primary + allow-bean-definition-overriding=true в test/application.yml
     * позволяют этому бину заменить JwtDecoder из SecurityConfig.
     *
     * Возвращает stub JWT — реальная проверка ролей в тестах идёт через
     * SecurityMockMvcRequestPostProcessors.jwt().authorities(...)
     */
    @Bean
    @Primary
    fun jwtDecoder(): JwtDecoder =
        JwtDecoder { token ->
            Jwt
                .withTokenValue(token)
                .header("alg", "RS256")
                .claim("sub", "test-user")
                .claim("scope", "openid")
                .claim("realm_access", mapOf("roles" to listOf("audit_read")))
                .issuedAt(Instant.now())
                .expiresAt(Instant.now().plusSeconds(3600))
                .build()
        }
}
