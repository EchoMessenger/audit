package com.echomessenger.audit.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.HttpMethod
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity
import org.springframework.security.config.annotation.web.builders.HttpSecurity
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity
import org.springframework.security.config.http.SessionCreationPolicy
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter
import org.springframework.security.web.SecurityFilterChain
import org.springframework.web.cors.CorsConfiguration
import org.springframework.web.cors.CorsConfigurationSource
import org.springframework.web.cors.UrlBasedCorsConfigurationSource

@Configuration
@EnableWebSecurity
@EnableMethodSecurity(prePostEnabled = true)
class SecurityConfig {
    @Bean
    fun securityFilterChain(http: HttpSecurity): SecurityFilterChain {
        http
            .csrf { it.disable() }
            .cors { }
            .sessionManagement { it.sessionCreationPolicy(SessionCreationPolicy.STATELESS) }
            .authorizeHttpRequests { auth ->
                // Actuator — доступен без токена (проверяется на отдельном порту 8081)
                auth.requestMatchers("/actuator/**").permitAll()

                // Preflight CORS запросы не должны требовать Bearer токен.
                auth.requestMatchers(HttpMethod.OPTIONS, "/**").permitAll()

                // Статус инцидентов — только admin
                auth
                    .requestMatchers(HttpMethod.POST, "/api/v1/incidents/*/status")
                    .hasRole("audit_admin")

                // Все audit/analytics эндпоинты требуют минимум audit:read
                auth.requestMatchers("/api/v1/**").hasAnyRole("audit_read", "audit_admin")

                auth.anyRequest().authenticated()
            }.oauth2ResourceServer { oauth2 ->
                oauth2.jwt { jwt ->
                    jwt.jwtAuthenticationConverter(keycloakJwtAuthConverter())
                }
            }

        return http.build()
    }

    /**
     * Маппинг Keycloak realm_access.roles → Spring GrantedAuthority с префиксом ROLE_.
     * Keycloak кладёт роли в claim realm_access.roles, не в стандартный scope.
     */
    @Bean
    fun keycloakJwtAuthConverter(): JwtAuthenticationConverter =
        JwtAuthenticationConverter().apply {
            setJwtGrantedAuthoritiesConverter { jwt ->
                val realmAccess = jwt.claims["realm_access"] as? Map<*, *>
                val roles = realmAccess?.get("roles") as? List<*> ?: emptyList<Any>()
                roles
                    .filterIsInstance<String>()
                    .filter { it.startsWith("audit") }
                    .map { SimpleGrantedAuthority("ROLE_${it.replace(":", "_")}") }
            }
        }

    @Bean
    fun corsConfigurationSource(): CorsConfigurationSource {
        val config = CorsConfiguration().apply {
            allowedOriginPatterns = listOf(
                "https://*.echo-messenger.ru",
                "http://localhost:*"
            )
            allowedMethods = listOf("GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS")
            allowedHeaders = listOf("Authorization", "Content-Type", "Accept", "Origin")
            exposedHeaders = listOf("Authorization")
            allowCredentials = true
            maxAge = 3600
        }

        return UrlBasedCorsConfigurationSource().apply {
            registerCorsConfiguration("/**", config)
        }
    }
}
