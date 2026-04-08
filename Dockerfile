# ── Stage 1: Dependencies cache ──────────────────────────────────────────────
FROM eclipse-temurin:21-jdk AS deps

WORKDIR /build

COPY gradlew gradlew
COPY gradle/ gradle/
COPY build.gradle.kts settings.gradle.kts ./

RUN chmod +x gradlew

# Кэшируем зависимости (самый дорогой шаг)
RUN ./gradlew dependencies --no-daemon

# ── Stage 2: Build ──────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jdk AS builder

WORKDIR /build

COPY --from=deps /root/.gradle /root/.gradle

COPY . .

RUN chmod +x gradlew

RUN ./gradlew bootJar \
    --no-daemon \
    -x test \
    --stacktrace

# ── Stage 3: Runtime ────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jre-alpine AS runtime

WORKDIR /app

# Устанавливаем wget для healthcheck
RUN apk add --no-cache wget

# Непривилегированный пользователь
RUN addgroup -S app && adduser -S app -G app

# Копируем jar (универсально, без захардкоженного имени)
COPY --from=builder /build/build/libs/*.jar app.jar

# Права
RUN chown -R app:app /app

USER app

# JVM оптимизации
ENV JAVA_OPTS="-XX:+UseContainerSupport \
-XX:MaxRAMPercentage=75.0 \
-XX:+UseG1GC \
-XX:+ExitOnOutOfMemoryError \
-Djava.security.egd=file:/dev/./urandom"

EXPOSE 8080

# Healthcheck (надёжный)
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 \
  CMD wget -qO- http://localhost:8080/actuator/health || exit 1

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]