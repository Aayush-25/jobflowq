# JobFlowQ

A Java / Spring Boot learning project for PostgreSQL-backed job processing, retries, and Kafka lifecycle events.

Job execution currently simulates work with timed delays; it does not send emails or generate reports. PostgreSQL is the queue, while Kafka records state-change events.

> **GitHub:** https://github.com/Aayush-25/jobflowq

## Architecture

```
                                                   ┌──────────────┐
  REST Client ──▶ JobController ──▶ JobService ──▶│  PostgreSQL  │
                                         │          └──────────────┘
                                         │ (Kafka event: CREATED / STATUS_UPDATED)
                                         ▼
                              ┌───────────────────────┐
                              │         Kafka         │
                              │ "job-application-     │
                              │   events" topic       │
                              └───────────────────────┘
                                    ▲          │
                                    │          ▼
                          JobWorkerService   JobEventConsumer
                       (claims, processes,    (logs every
                        retries, dead-letters  received event)
                        jobs; publishes its
                        own STATUS_UPDATED
                        events on every
                        transition)
```

`JobService` publishes a `CREATED` event when a job is submitted (and a
`STATUS_UPDATED` event on cancellation). `JobWorkerService` independently
publishes `STATUS_UPDATED` events on every transition it drives — claiming
a job, completing it, retrying it, or dead-lettering it. Both producers
write to the same `job-application-events` topic; `JobEventConsumer`
subscribes to it and logs everything it receives.

## Tech Stack

![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.5.15-brightgreen)
![Kafka](https://img.shields.io/badge/Kafka-3.7-black)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)
![Java](https://img.shields.io/badge/Java-17-orange)

## Features

- **Priority queue on Postgres** — workers dequeue jobs with `SELECT ...
  FOR UPDATE SKIP LOCKED`, ordered by `priority DESC, created_at ASC`, to express non-blocking row selection. Correct multi-worker behavior depends on the transaction boundary and must be validated with integration tests; this is not an exactly-once guarantee.
- **Event-driven** — every job creation, cancellation, and worker-driven
  status transition publishes a `JobApplicationEvent` to Kafka.
- **Kafka in KRaft mode** — no Zookeeper; a single-node broker handles
  both controller and broker roles.
- **Swagger UI** — interactive API docs and a browsable OpenAPI schema,
  no extra setup required.
- **Retry + dead-letter handling** — failed jobs return to pending until
  the failure count reaches `maxRetries`; at that threshold, they're marked `DEAD` instead of
  being retried forever or silently dropped.

## How to Run Locally

**Prerequisites:** Docker with Compose. For host-side tests, use Java 17+ and the included Maven wrapper.

```bash
git clone https://github.com/Aayush-25/jobflowq.git
cd jobflowq
cp .env.example .env
```

Edit `.env` before starting: set `POSTGRES_DB`, `POSTGRES_USER`, and `POSTGRES_PASSWORD`, then give `SPRING_DATASOURCE_USERNAME` and `SPRING_DATASOURCE_PASSWORD` the same user/password values. The example file contains placeholders.

Bring up the full stack — Postgres, Kafka, and the app — in one command:

```bash
docker compose up --build
```

This builds the app image, waits for Postgres and Kafka to report
healthy, then starts the app. The API is available at
`http://localhost:8080` once `jobflowq-app` is up.

**Submit a job:**

```bash
curl -X POST http://localhost:8080/api/jobs \
  -H "Content-Type: application/json" \
  -d '{"type": "EMAIL", "companyName": "Acme Corp", "payload": "{\"to\":\"user@example.com\"}", "priority": 10, "maxRetries": 3}'
```

**Fetch a job:**

```bash
curl http://localhost:8080/api/jobs/1
```

**List all jobs:**

```bash
curl http://localhost:8080/api/jobs
```

**Cancel a pending job:**

```bash
curl -X DELETE http://localhost:8080/api/jobs/1
```

**Check queue metrics:**

```bash
curl http://localhost:8080/api/metrics
```

**Browse the API interactively:** open `http://localhost:8080/swagger-ui.html`.

### Running on the host

Start only the infrastructure with `docker compose up -d postgres kafka`. Export `SPRING_DATASOURCE_URL` (using `localhost:5432` and your database name), `SPRING_DATASOURCE_USERNAME`, and `SPRING_DATASOURCE_PASSWORD` to match `.env`, then run `sh ./mvnw spring-boot:run`. Spring Boot does not automatically load the Compose `.env` file.

The defaults in `application.properties` are local-development examples; do not assume they match the credentials you chose.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/jobs` | Submit a new job to the queue |
| GET | `/api/jobs/{id}` | Retrieve a job by its ID |
| GET | `/api/jobs` | List all jobs in the queue |
| DELETE | `/api/jobs/{id}` | Cancel a pending job |
| GET | `/api/metrics` | Retrieve aggregate queue metrics |

## Kafka Event Flow

Every job lifecycle change publishes a `JobApplicationEvent` to the
`job-application-events` topic:

```java
record JobApplicationEvent(
    Long applicationId,   // the job's id
    String companyName,   // optional, set at submission
    JobStatus status,      // the job's status at publish time
    LocalDateTime updatedAt,
    String eventType       // "CREATED" or "STATUS_UPDATED"
)
```

| Event | `eventType` | Fired by | When |
|---|---|---|---|
| Job created | `CREATED` | `JobService` | Once, when a job is submitted |
| Job cancelled | `STATUS_UPDATED` | `JobService` | When a pending job is cancelled (→ `FAILED`) |
| Job claimed | `STATUS_UPDATED` | `JobWorkerService` | When a worker picks up a pending job (→ `PROCESSING`) |
| Job completed | `STATUS_UPDATED` | `JobWorkerService` | On successful processing (→ `COMPLETED`) |
| Job retried | `STATUS_UPDATED` | `JobWorkerService` | On failure, below the retry limit (→ `PENDING`) |
| Job dead-lettered | `STATUS_UPDATED` | `JobWorkerService` | On failure, retry limit exhausted (→ `DEAD`) |

The message key is the job's id (as a string), so all events for the same
job land on the same partition and are consumed in order. Publishing is
best-effort: a Kafka outage is logged but never blocks a job from being
created, processed, or cancelled. `JobEventConsumer` is a simple logging
listener that proves the round trip; it has no other side effects.

## Environment Variables

| Variable | Description | Example Value |
|----------|--------------|----------------|
| `SPRING_DATASOURCE_URL` | JDBC URL for the Postgres database | `jdbc:postgresql://postgres:5432/jobflowq` |
| `SPRING_DATASOURCE_USERNAME` | Postgres username | `admin` |
| `SPRING_DATASOURCE_PASSWORD` | Postgres password | `password` |
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address | `kafka:29092` |
| `WORKER_ID` | Identifies which worker instance processed a job, recorded on the `Job` entity | `worker-1` |

These are set as environment variables on the `app` service in
`docker-compose.yml` and override `application.properties` via Spring
Boot's relaxed env-var binding — the properties file itself reflects the
host defaults (`localhost:5432`/`localhost:9092`) for running outside
Docker.

## Tests

The isolated service and Kafka unit tests use mocks and do not need running infrastructure:

```bash
sh ./mvnw -B '-Dtest=JobServiceTest,JobWorkerServiceTest,JobEventProducerTest,JobEventConsumerTest' test
```

The complete `sh ./mvnw test` command also runs `JobflowqApplicationTests`, which loads the Spring application context. Configure the database and start PostgreSQL/Kafka before running it. Unit tests do not prove concurrent job claiming or Kafka delivery against real services.

## Current limitations and next steps

- Processing is simulated with `Thread.sleep`; application-specific job handlers are not implemented.
- `processNextJob()` is annotated as transactional but is called from the same service by the scheduler. Spring proxy transactions do not apply to self-invocation; verify and correct the transaction boundary before relying on row locks across multiple workers. See the [Spring transaction documentation](https://docs.spring.io/spring-framework/reference/data-access/transaction/declarative/annotations.html).
- Kafka publishing is best-effort, without a transactional outbox; database state and event publication can diverge.
- `maxRetries` acts as a failure threshold: reaching it marks the job `DEAD`. There is no retry backoff or automatic recovery of abandoned `PROCESSING` jobs.
- Authentication, public-service hardening, and load/concurrency testing remain future work.

These are useful next engineering exercises: prove the locking behavior with a real PostgreSQL test, add crash recovery, and replace simulated work with one real handler.
