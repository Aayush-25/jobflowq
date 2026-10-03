# App Containerization + README Rewrite

## Context

JobFlowQ's `docker-compose.yml` currently only runs Postgres and Kafka; the
Spring Boot app itself runs on the host via `./mvnw spring-boot:run`,
relying on `application.properties` hardcoding `localhost:5432` and
`localhost:9092`. This was triggered by a request for a production-quality
README whose "how to run" section says `docker-compose up --build` — which
requires the app to actually be part of the compose stack. Making that true
surfaces a real networking issue: Kafka's only configured listener
advertises `localhost:9092`, which is unreachable from another container on
the Docker network (a container's `localhost` is itself, not the Kafka
container).

## Goals

- Add a multi-stage `Dockerfile` that builds and runs the Spring Boot app.
- Add an `app` service to `docker-compose.yml` that builds from that
  Dockerfile, depends on healthy Postgres and Kafka, and connects to both
  via Docker network hostnames.
- Fix Kafka's listener config to advertise a second, internal listener
  (`kafka:29092`) for container-to-container traffic, while leaving the
  existing host-facing `localhost:9092` listener unchanged.
- Add healthchecks to `postgres` and `kafka` so `depends_on:
  condition: service_healthy` is meaningful.
- Add a `.dockerignore` so the build context excludes `target/`, `.git/`,
  `.idea/`, etc.
- Rewrite `README.md` completely with the 8 sections specified by the user
  (title/description, ASCII architecture diagram, tech badges, features,
  how-to-run with `docker-compose up --build` + curl examples, API
  endpoint table, Kafka event flow section, environment variables table).

## Non-goals

- No change to `application.properties` itself — host-based `./mvnw
  spring-boot:run` must keep working exactly as today, unchanged.
- No change to any application code (controllers, services, DTOs).
- No CI/CD pipeline, no image publishing/registry — local
  `docker-compose up --build` only.

## Design

### 1. Dockerfile

New file `Dockerfile` at the project root:
```dockerfile
# Build stage
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn -q package -DskipTests

# Runtime stage
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /app/target/jobflowq-0.0.1-SNAPSHOT.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
```

### 2. .dockerignore

New file `.dockerignore`:
```
target/
.git/
.idea/
.mvn/
docs/
.superpowers/
*.md
```

### 3. docker-compose.yml changes

**Kafka service** gains a second listener for internal Docker-network
traffic, alongside the existing host-facing one:
```yaml
  kafka:
    image: apache/kafka:3.7.0
    container_name: jobflowq-kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,BROKER://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,BROKER://kafka:29092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,BROKER:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: BROKER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 || exit 1"]
      interval: 10s
      timeout: 10s
      retries: 10
```
`PLAINTEXT://localhost:9092` still works unchanged from the host (this is
what the existing manual verification steps used). `BROKER://kafka:29092`
is the new listener other containers use; `KAFKA_INTER_BROKER_LISTENER_NAME`
tells Kafka which listener to use for its own internal broker traffic
(required even in a single-node setup).

**Postgres service** gains a healthcheck:
```yaml
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U admin -d jobflowq"]
      interval: 5s
      timeout: 5s
      retries: 10
```

**New `app` service:**
```yaml
  app:
    build: .
    container_name: jobflowq-app
    ports:
      - "8080:8080"
    environment:
      SPRING_DATASOURCE_URL: jdbc:postgresql://postgres:5432/jobflowq
      SPRING_DATASOURCE_USERNAME: admin
      SPRING_DATASOURCE_PASSWORD: password
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:29092
    depends_on:
      postgres:
        condition: service_healthy
      kafka:
        condition: service_healthy
```
All four env vars override the corresponding `application.properties`
entries via Spring Boot's relaxed env-var binding
(`SPRING_DATASOURCE_URL` → `spring.datasource.url`, etc.) — the properties
file itself is untouched, so host-based `./mvnw spring-boot:run` still
reads its own `localhost` defaults unchanged.

### 4. README.md

Full rewrite per the user's 8-point spec:
1. Title + one-line description.
2. ASCII diagram: `REST Client → JobController → JobService → PostgreSQL`,
   branching to `JobService ↓ (Kafka event) → JobWorkerService →
   job-application-events topic → JobEventConsumer` (worker also publishes
   on its own transitions, not just relaying JobService's).
3. Badges: Spring Boot, Kafka, PostgreSQL, Docker, Java 17 (shields.io).
4. Features: priority queue (`SKIP LOCKED`), event-driven (Kafka), Kafka
   KRaft mode, Swagger UI, retry/dead-letter.
5. How to run: `docker-compose up --build` (full stack — Postgres, Kafka,
   app), then curl examples against `localhost:8080`.
6. API endpoint table: the 5 existing endpoints.
7. Kafka event flow: `CREATED` fires once on submit; `STATUS_UPDATED`
   fires on cancel and on every worker transition (claim/complete/retry/
   dead-letter), each carrying `applicationId` (= Job id), `companyName`,
   `status`, `updatedAt`, `eventType`.
8. Environment variables table: the 4 compose-overridable Spring
   properties above, plus `WORKER_ID` (already read via `System.getenv` in
   `JobWorkerService`, defaults to `worker-1`).

## Testing

- `docker compose up --build` brings up all three services; `docker
  compose ps` shows all three `healthy`/`Up`.
- `curl localhost:8080/api/jobs` (and the submit/cancel/metrics endpoints)
  work identically to running the app via Maven on the host.
- Application logs show the Kafka consumer successfully subscribing to
  `job-application-events` (no `UNKNOWN_TOPIC_OR_PARTITION` retries
  hanging indefinitely, no repeated connection-refused errors).
- `./mvnw spring-boot:run` on the host (outside Docker, with `docker
  compose up -d postgres kafka` for just the infra) still works exactly as
  before — confirms `application.properties` itself wasn't touched.
