# Externalize docker-compose Credentials Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the 5 hardcoded credentials in `docker-compose.yml` into a gitignored `.env` file, with a committed `.env.example` template.

**Architecture:** Two new files (`.env`, `.env.example`) hold the same key set; `docker-compose.yml` references them via `${VAR_NAME}` substitution, which Docker Compose auto-loads from `.env` in the same directory — no `env_file:` directive needed.

**Tech Stack:** Docker Compose `${VAR}` substitution.

## Global Constraints

- Exact variable names: `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `SPRING_DATASOURCE_USERNAME`, `SPRING_DATASOURCE_PASSWORD`.
- `.env` real values: `jobflowq` / `admin` / `password` / `admin` / `password` (unchanged from current hardcoded values).
- `.env` is gitignored; `.env.example` is committed with placeholder values.
- The `postgres` healthcheck and the `app` service's `SPRING_DATASOURCE_URL` must also reference `${POSTGRES_USER}`/`${POSTGRES_DB}` instead of re-hardcoding `admin`/`jobflowq`, to avoid drift.
- No changes to `application.properties` or Java code.

---

### Task 1: Create .env and .env.example

**Files:**
- Create: `.env`
- Create: `.env.example`

**Interfaces:**
- Produces: the 5 variables, consumed by Task 2's `docker-compose.yml` via `${VAR_NAME}`.

- [ ] **Step 1: Create .env**

Create `.env` at the project root:

```
POSTGRES_DB=jobflowq
POSTGRES_USER=admin
POSTGRES_PASSWORD=password
SPRING_DATASOURCE_USERNAME=admin
SPRING_DATASOURCE_PASSWORD=password
```

- [ ] **Step 2: Create .env.example**

Create `.env.example` at the project root:

```
POSTGRES_DB=your_db_name
POSTGRES_USER=your_db_user
POSTGRES_PASSWORD=your_db_password
SPRING_DATASOURCE_USERNAME=your_db_user
SPRING_DATASOURCE_PASSWORD=your_db_password
```

- [ ] **Step 3: Commit .env.example only**

```bash
git add .env.example
git commit -m "feat: add .env.example template for docker-compose credentials"
```

(`.env` itself is not committed — it gets ignored in Task 2.)

---

### Task 2: Reference variables in docker-compose.yml, ignore .env

**Files:**
- Modify: `docker-compose.yml`
- Modify: `.gitignore`

**Interfaces:**
- Consumes: the 5 variables from Task 1's `.env`.

- [ ] **Step 1: Update docker-compose.yml**

Replace the full contents of `docker-compose.yml`:

```yaml
version: '3.8'
services:
  postgres:
    image: postgres:15
    container_name: jobflowq-db
    environment:
      POSTGRES_DB: ${POSTGRES_DB}
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB}"]
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
      SPRING_DATASOURCE_URL: jdbc:postgresql://postgres:5432/${POSTGRES_DB}
      SPRING_DATASOURCE_USERNAME: ${SPRING_DATASOURCE_USERNAME}
      SPRING_DATASOURCE_PASSWORD: ${SPRING_DATASOURCE_PASSWORD}
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:29092
    depends_on:
      postgres:
        condition: service_healthy
      kafka:
        condition: service_healthy

volumes:
  postgres_data:
```

- [ ] **Step 2: Add .env to .gitignore**

In `.gitignore`, append:

```
.env
```

- [ ] **Step 3: Verify compose resolves all variables with no warnings**

Run: `docker compose config -q`
Expected: exit code 0, no "variable is not set" warnings.

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml .gitignore
git commit -m "feat: reference credentials via env vars in docker-compose.yml"
```

---

### Task 3: Full verification

**Files:** none (verification only)

- [ ] **Step 1: Confirm git status**

Run: `git status --short`
Expected: `.env` does not appear (ignored); working tree otherwise clean.

- [ ] **Step 2: Bring up the full stack**

Run: `docker compose up --build -d`
Expected: all 3 services start; `postgres` and `kafka` report `healthy`.

- [ ] **Step 3: Confirm the app connects with the substituted credentials**

Run: `curl -s -X POST http://localhost:8080/api/jobs -H "Content-Type: application/json" -d '{"type":"EMAIL"}'`
Expected: `201` response — confirms the app's DB connection (via `${SPRING_DATASOURCE_USERNAME}`/`${SPRING_DATASOURCE_PASSWORD}`/the templated JDBC URL) works identically to before.

- [ ] **Step 4: Tear down**

Run: `docker compose down`

- [ ] **Step 5: Report results**
