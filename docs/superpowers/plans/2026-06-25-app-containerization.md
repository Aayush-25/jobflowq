# App Containerization + README Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `docker-compose up --build` run the full JobFlowQ stack (Postgres + Kafka + app), fix the Kafka Docker-networking gap that blocks it, and rewrite README.md to document the now-true setup.

**Architecture:** A multi-stage `Dockerfile` builds and runs the Spring Boot app; `docker-compose.yml` gains an `app` service plus healthchecks on `postgres`/`kafka` so `depends_on: condition: service_healthy` is meaningful; Kafka gains a second internal listener (`kafka:29092`) so containers can reach it (the existing host-facing `localhost:9092` listener is untouched). README.md is rewritten last, once the stack is verified working.

**Tech Stack:** Docker, Docker Compose, `maven:3.9-eclipse-temurin-17`, `eclipse-temurin:17-jre-alpine`, Spring Boot relaxed env-var binding.

## Global Constraints

- `application.properties` itself must not change — host-based `./mvnw spring-boot:run` must keep working unchanged, reading its own `localhost` defaults.
- No application code changes (controllers, services, DTOs).
- Existing host-facing Kafka listener `PLAINTEXT://localhost:9092` and Postgres port `5432` must keep working unchanged for host access.
- New internal Kafka listener for container traffic: `BROKER://kafka:29092`.
- App service env vars: `SPRING_DATASOURCE_URL=jdbc:postgresql://postgres:5432/jobflowq`, `SPRING_DATASOURCE_USERNAME=admin`, `SPRING_DATASOURCE_PASSWORD=password`, `SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka:29092`.
- Jar produced by the build is `target/jobflowq-0.0.1-SNAPSHOT.jar` (matches `pom.xml`'s `artifactId`/`version`).

---

### Task 1: Dockerfile + .dockerignore

**Files:**
- Create: `Dockerfile`
- Create: `.dockerignore`

**Interfaces:**
- Produces: a buildable image that runs the app on port 8080 — consumed by Task 2's `app` service (`build: .`).

- [ ] **Step 1: Create the Dockerfile**

Create `Dockerfile` at the project root:

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

- [ ] **Step 2: Create .dockerignore**

Create `.dockerignore` at the project root:

```
target/
.git/
.idea/
.mvn/
docs/
.superpowers/
*.md
```

- [ ] **Step 3: Build the image standalone to verify it builds**

Run: `docker build -t jobflowq-app-test .`
Expected: build succeeds, ends with `Successfully tagged jobflowq-app-test:latest` (or the buildkit equivalent final success line).

- [ ] **Step 4: Commit**

```bash
git add Dockerfile .dockerignore
git commit -m "feat: add multi-stage Dockerfile for the Spring Boot app"
```

---

### Task 2: docker-compose.yml — Kafka dual listeners, healthchecks, app service

**Files:**
- Modify: `docker-compose.yml`

**Interfaces:**
- Consumes: the image built by Task 1's `Dockerfile` (referenced via `build: .`).
- Produces: a full 3-service stack (`postgres`, `kafka`, `app`) — consumed by Task 4's verification.

- [ ] **Step 1: Replace docker-compose.yml**

Replace the full contents of `docker-compose.yml`:

```yaml
version: '3.8'
services:
  postgres:
    image: postgres:15
    container_name: jobflowq-db
    environment:
      POSTGRES_DB: jobflowq
      POSTGRES_USER: admin
      POSTGRES_PASSWORD: password
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U admin -d jobflowq"]
      interval: 5s
      timeout: 5s
      retries: 10

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

volumes:
  postgres_data:
```

- [ ] **Step 2: Verify it compiles/parses**

Run: `docker compose config -q`
Expected: exits with no output and exit code 0 (validates YAML + schema without starting anything).

- [ ] **Step 3: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: add app service to docker-compose, fix Kafka listener for container networking"
```

---

### Task 3: Full-stack verification

**Files:** none (verification only)

**Interfaces:**
- Consumes: Tasks 1 and 2.

- [ ] **Step 1: Stop any host-run instance of the app and any existing compose stack**

Run: `lsof -tiTCP:8080 -sTCP:LISTEN | xargs -r kill; docker compose down`

- [ ] **Step 2: Bring up the full stack**

Run: `docker compose up --build -d`
Expected: builds the app image, then starts all three containers.

- [ ] **Step 3: Wait for and confirm all three services are healthy**

Run: `docker compose ps`
Expected: `jobflowq-db` and `jobflowq-kafka` show `healthy`, `jobflowq-app` shows `Up` (app has no healthcheck defined, so just confirm it's running and didn't restart-loop).

If `jobflowq-app` is restarting or exited, run `docker compose logs app` and diagnose before continuing — do not proceed to curl tests against a broken container.

- [ ] **Step 4: Confirm the app is reachable and the DB connection works**

Run: `curl -s -X POST http://localhost:8080/api/jobs -H "Content-Type: application/json" -d '{"type":"EMAIL","companyName":"Acme Corp"}'`
Expected: `201`-style JSON body with `"companyName":"Acme Corp"`, `"status":"PENDING"`.

- [ ] **Step 5: Confirm the Kafka event flow works inside the container network**

Run: `docker compose logs app | grep "Received job event"`
Expected: at least one `JobEventConsumer` log line containing `eventType=CREATED` for the job submitted in Step 4, followed shortly by `eventType=STATUS_UPDATED` lines as the worker (running inside the same `app` container) claims and completes it. If these lines are absent or show repeated `UNKNOWN_TOPIC_OR_PARTITION`/connection-refused errors, the listener fix from Task 2 did not take effect — stop and diagnose rather than proceeding.

- [ ] **Step 6: Confirm existing read endpoints still work**

Run:
```bash
curl -s http://localhost:8080/api/jobs
curl -s http://localhost:8080/api/metrics
curl -s -o /dev/null -w "%{http_code}\n" -L http://localhost:8080/swagger-ui.html
```
Expected: `200` for all three (job list, metrics, swagger UI).

- [ ] **Step 7: Tear down the compose stack**

Run: `docker compose down`

- [ ] **Step 8: Confirm host-based run still works unchanged (application.properties wasn't touched)**

Run: `docker compose up -d postgres kafka` (infra only), wait a few seconds, then `./mvnw -q test`
Expected: `BUILD SUCCESS` — confirms the existing test suite (which relies on `application.properties`'s `localhost` defaults) is unaffected by the compose/Dockerfile changes.

Run: `docker compose down` again to clean up.

- [ ] **Step 9: Report results**

Summarize pass/fail for each step. If any step failed, stop and report rather than proceeding to Task 4.

---

### Task 4: README.md rewrite

**Files:**
- Modify: `README.md` (full replacement)

**Interfaces:**
- Consumes: the verified, working `docker-compose up --build` flow from Task 3 — the README's instructions must match what was actually verified, not what was merely intended.

- [ ] **Step 1: Replace README.md**

Replace the full contents of `README.md` with a document covering exactly these 8 sections, in this order:

1. **Title + one-line description** — `# JobFlowQ` followed by a single sentence describing it as an event-driven distributed job queue built on PostgreSQL and Kafka.
2. **ASCII architecture diagram** showing: `REST Client → JobController → JobService → PostgreSQL`, with a branch `JobService ↓ (Kafka event)`, and a second flow `JobWorkerService → job-application-events topic → JobEventConsumer` (note in the diagram or surrounding text that `JobWorkerService` also publishes its own events on every transition, not only relaying `JobService`'s).
3. **Tech stack badges** — shields.io badges for Spring Boot, Kafka, PostgreSQL, Docker, Java 17, e.g.:
   ```markdown
   ![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.5.15-brightgreen)
   ![Kafka](https://img.shields.io/badge/Kafka-3.9-black)
   ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)
   ![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)
   ![Java](https://img.shields.io/badge/Java-17-orange)
   ```
4. **Features list** — priority queue with `SELECT ... FOR UPDATE SKIP LOCKED`, event-driven Kafka publishing, Kafka running in KRaft mode (no Zookeeper), Swagger UI for API docs, retry + dead-letter handling.
5. **How to run locally** — `docker-compose up --build` brings up Postgres, Kafka, and the app together; then curl examples for submit/get/list/cancel/metrics against `http://localhost:8080`.
6. **API endpoints table** — the 5 existing endpoints (method, path, description), matching `JobController`/`MetricsController` exactly:

   | Method | Path | Description |
   |--------|------|-------------|
   | POST | `/api/jobs` | Submit a new job to the queue |
   | GET | `/api/jobs/{id}` | Retrieve a job by its ID |
   | GET | `/api/jobs` | List all jobs in the queue |
   | DELETE | `/api/jobs/{id}` | Cancel a pending job |
   | GET | `/api/metrics` | Retrieve aggregate queue metrics |

7. **Kafka event flow section** — explain that every job publishes a `JobApplicationEvent` (fields: `applicationId`, `companyName`, `status`, `updatedAt`, `eventType`) to the `job-application-events` topic: `eventType="CREATED"` fires exactly once, when a job is submitted; `eventType="STATUS_UPDATED"` fires on cancellation and on every worker-driven transition (claimed→PROCESSING, completed→COMPLETED, retried→PENDING, dead-lettered→DEAD). A logging `JobEventConsumer` consumes the topic.
8. **Environment variables table** — the variables introduced in Task 2 plus `WORKER_ID`:

   | Variable | Default | Description |
   |----------|---------|-------------|
   | `SPRING_DATASOURCE_URL` | `jdbc:postgresql://localhost:5432/jobflowq` (host) / `jdbc:postgresql://postgres:5432/jobflowq` (docker-compose) | JDBC URL for the Postgres database |
   | `SPRING_DATASOURCE_USERNAME` | `admin` | Postgres username |
   | `SPRING_DATASOURCE_PASSWORD` | `password` | Postgres password |
   | `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` (host) / `kafka:29092` (docker-compose) | Kafka broker address |
   | `WORKER_ID` | `worker-1` | Identifies which worker instance processed a job, recorded on the `Job` entity |

- [ ] **Step 2: Spot-check the README against the verified behavior**

Re-read the rewritten README's "How to run locally" section against the actual commands run and their actual output in Task 3. Fix any mismatch before committing — the README must describe what Task 3 verified, not a hypothetical.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: rewrite README with architecture diagram, Kafka event flow, and full docker-compose setup"
```
