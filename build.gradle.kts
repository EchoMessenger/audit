plugins {
    id("org.springframework.boot") version "3.4.2"
    id("io.spring.dependency-management") version "1.1.7"
    kotlin("jvm") version "2.1.0"
    kotlin("plugin.spring") version "2.1.0"
}

group = "com.echomessenger"
version = "0.0.1-SNAPSHOT"

// Use toolchain instead of sourceCompatibility
java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

repositories {
    mavenCentral()
}

val testcontainersVersion = "1.20.4"
extra["testcontainersVersion"] = testcontainersVersion

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-security")
    implementation("org.springframework.boot:spring-boot-starter-oauth2-resource-server")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.boot:spring-boot-starter-cache")
    implementation("org.springframework.boot:spring-boot-starter-jdbc")

    // Kotlin
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    // Coroutines
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor:1.10.1")

    // ClickHouse
    implementation("com.clickhouse:clickhouse-jdbc:0.7.1")
    implementation("com.clickhouse:clickhouse-http-client:0.7.1")
    implementation("org.apache.httpcomponents.client5:httpclient5:5.4.1")
    implementation("at.yawk.lz4:lz4-java:1.10.1")

    // Flyway
    implementation("org.flywaydb:flyway-core:11.2.0")

    // Rate limiting
    implementation("com.bucket4j:bucket4j_jdk17-core:8.17.0")
    implementation("com.github.ben-manes.caffeine:caffeine:3.2.0")

    // Metrics & Observability
    implementation("io.micrometer:micrometer-registry-prometheus")
    implementation("io.micrometer:micrometer-tracing-bridge-otel")

    // Object storage (S3 / MinIO)
    implementation(platform("software.amazon.awssdk:bom:2.32.18"))
    implementation("software.amazon.awssdk:s3")

    // Testing
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.security:spring-security-test")
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:clickhouse:$testcontainersVersion")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.1")
    testImplementation("io.mockk:mockk:1.13.16")
    testImplementation("com.ninja-squad:springmockk:4.0.2")
    testImplementation("com.squareup.okhttp3:mockwebserver:4.12.0")
}

configurations.all {
    resolutionStrategy.capabilitiesResolution.withCapability("org.lz4:lz4-java") {
        select("at.yawk.lz4:lz4-java:1.10.1")
    }
}

dependencyManagement {
    imports {
        mavenBom("org.testcontainers:testcontainers-bom:$testcontainersVersion")
    }
}

kotlin {
    compilerOptions {
        freeCompilerArgs.add("-Xjsr305=strict")
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_21)
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}

tasks.bootJar {
    archiveFileName.set("audit-service.jar")
}
