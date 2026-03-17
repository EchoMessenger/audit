# ── Stage 1: Build ───────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jdk-alpine AS builder

WORKDIR /build

# Кэшируем зависимости отдельным слоем — пересборка только при изменении build файлов
COPY build.gradle.kts settings.gradle.kts ./
COPY gradle/ gradle/
COPY gradlew ./

RUN chmod +x gradlew && ./gradlew dependencies --no-daemon --quiet 2>/dev/null || true

COPY src/ src/

RUN ./gradlew bootJar --no-daemon -x test

# ── Stage 2: Runtime ─────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jre-alpine AS runtime

WORKDIR /app

# Непривилегированный пользователь
RUN addgroup -S audit && adduser -S audit -G audit

# Директория для PVC exports
RUN mkdir -p /exports && chown audit:audit /exports

COPY --from=builder /build/build/libs/audit-service.jar app.jar

# Оптимизация JVM для контейнеров:
# UseContainerSupport — автоматически определяет лимиты памяти k8s
# MaxRAMPercentage  — использует 75% от container memory limit
ENV JAVA_OPTS="-XX:+UseContainerSupport \
               -XX:MaxRAMPercentage=75.0 \
               -XX:+UseG1GC \
               -XX:+ExitOnOutOfMemoryError \
               -Djava.security.egd=file:/dev/./urandom"

USER audit

EXPOSE 8080 8081

HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 \
  CMD wget -qO- http://localhost:8081/actuator/health/liveness || exit 1

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]
