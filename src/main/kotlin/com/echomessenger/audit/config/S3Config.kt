package com.echomessenger.audit.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.S3Configuration
import software.amazon.awssdk.services.s3.presigner.S3Presigner
import java.net.URI
import java.time.Duration

@Configuration
@ConditionalOnProperty(prefix = "audit.export", name = ["storage-type"], havingValue = "s3")
class S3Config(
    @Value("\${audit.export.s3.region:us-east-1}") private val region: String,
    @Value("\${audit.export.s3.endpoint:}") private val endpoint: String,
    @Value("\${audit.export.s3.public-endpoint:}") private val publicEndpoint: String,
    @Value("\${audit.export.s3.access-key:}") private val accessKey: String,
    @Value("\${audit.export.s3.secret-key:}") private val secretKey: String,
    @Value("\${audit.export.s3.path-style-access:false}") private val pathStyleAccess: Boolean,
    @Value("\${audit.export.s3.max-connections:50}") private val maxConnections: Int,
    @Value("\${audit.export.s3.connection-timeout-ms:3000}") private val connectionTimeoutMs: Long,
    @Value("\${audit.export.s3.socket-timeout-ms:5000}") private val socketTimeoutMs: Long,
    @Value("\${audit.export.s3.connection-acquisition-timeout-ms:3000}") private val connectionAcquisitionTimeoutMs: Long,
    @Value("\${audit.export.s3.connection-max-idle-ms:20000}") private val connectionMaxIdleMs: Long,
    @Value("\${audit.export.s3.connection-ttl-ms:60000}") private val connectionTtlMs: Long,
) {
    private fun shouldUsePathStyleAccess(): Boolean =
        pathStyleAccess || endpoint.isNotBlank() || publicEndpoint.isNotBlank()

    @Bean
    fun s3Client(): S3Client {
        val usePathStyleAccess = shouldUsePathStyleAccess()
        val httpClient =
            ApacheHttpClient.builder()
                .maxConnections(maxConnections)
                .connectionTimeout(Duration.ofMillis(connectionTimeoutMs))
                .socketTimeout(Duration.ofMillis(socketTimeoutMs))
                .connectionAcquisitionTimeout(Duration.ofMillis(connectionAcquisitionTimeoutMs))
                .connectionMaxIdleTime(Duration.ofMillis(connectionMaxIdleMs))
                .connectionTimeToLive(Duration.ofMillis(connectionTtlMs))
                .useIdleConnectionReaper(true)
                .build()

        val builder =
            S3Client.builder()
                .region(Region.of(region))
                .httpClient(httpClient)
                .overrideConfiguration(
                    ClientOverrideConfiguration.builder()
                        .apiCallAttemptTimeout(Duration.ofMillis(socketTimeoutMs + connectionTimeoutMs))
                        .apiCallTimeout(Duration.ofMillis((socketTimeoutMs + connectionTimeoutMs) * 2))
                        .build(),
                )
                .serviceConfiguration(
                    S3Configuration.builder()
                        .pathStyleAccessEnabled(usePathStyleAccess)
                        .build(),
                )

        if (endpoint.isNotBlank()) {
            builder.endpointOverride(URI.create(endpoint))
        }

        if (accessKey.isNotBlank() && secretKey.isNotBlank()) {
            builder.credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKey, secretKey),
                ),
            )
        } else {
            builder.credentialsProvider(DefaultCredentialsProvider.builder().build())
        }

        return builder.build()
    }

    @Bean
    fun s3Presigner(): S3Presigner {
        val usePathStyleAccess = shouldUsePathStyleAccess()
        val builder =
            S3Presigner.builder()
                .region(Region.of(region))
                .serviceConfiguration(
                    S3Configuration.builder()
                        .pathStyleAccessEnabled(usePathStyleAccess)
                        .build(),
                )
        val presignEndpoint = if (publicEndpoint.isNotBlank()) publicEndpoint else endpoint

        if (presignEndpoint.isNotBlank()) {
            builder.endpointOverride(URI.create(presignEndpoint))
        }

        if (accessKey.isNotBlank() && secretKey.isNotBlank()) {
            builder.credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKey, secretKey),
                ),
            )
        } else {
            builder.credentialsProvider(DefaultCredentialsProvider.builder().build())
        }

        return builder.build()
    }
}
