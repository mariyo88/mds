# ─── Stage 1: Build ──────────────────────────────────────────────────────────
# Uses eclipse-temurin:21-jdk (Debian-based) for the build stage because
# the Alpine variant has package conflicts with Maven on some registries.
# The runtime stage uses the slim JRE image to keep the final image small.
FROM eclipse-temurin:21-jdk AS build

WORKDIR /workspace

# Install Maven
RUN apt-get update && apt-get install -y --no-install-recommends maven && rm -rf /var/lib/apt/lists/*

# Copy pom files first — Docker layer cache means dependency resolution is
# only re-run when pom files change, not on every source change
COPY pom.xml .
COPY shared-models/pom.xml shared-models/
COPY order-api/pom.xml order-api/
COPY inventory-service/pom.xml inventory-service/

# Pre-download all dependencies into the image layer
RUN mvn dependency:go-offline -pl shared-models,order-api -am -q 2>/dev/null || true

# Copy source and build the fat JAR
COPY shared-models/src shared-models/src
COPY order-api/src order-api/src

RUN mvn install -pl shared-models,order-api -am -DskipTests -q

# ─── Stage 2: Runtime ────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jre AS runtime

# Non-root user for security
RUN groupadd -r appgroup && useradd -r -g appgroup appuser
USER appuser

WORKDIR /app

COPY --from=build /workspace/order-api/target/order-api-*.jar app.jar

EXPOSE 8080

# JVM flags: container-aware memory, prefer IPv4
ENTRYPOINT ["java", \
  "-XX:+UseContainerSupport", \
  "-XX:MaxRAMPercentage=75.0", \
  "-Djava.net.preferIPv4Stack=true", \
  "-jar", "app.jar"]
