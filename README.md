# JobFlowQ

An event-driven distributed job queue built on PostgreSQL and Kafka.

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
  FOR UPDATE SKIP LOCKED`, ordered by `priority DESC, created_at ASC`, so
  multiple worker instances can poll the same table concurrently with no
  double-processing and no external lock manager.
- **Event-driven** — every job creation, cancellation, and worker-driven
  status transition publishes a `JobApplicationEvent` to Kafka.
- **Kafka in KRaft mode** — no Zookeeper; a single-node broker handles
  both controller and broker roles.
- **Swagger UI** — interactive API docs and a browsable OpenAPI schema,
  no extra setup required.
- **Retry + dead-letter handling** — failed jobs are retried up to
  `maxRetries` times; once exhausted, they're marked `DEAD` instead of
  being retried forever or silently dropped.

## How to Run Locally

**Prerequisites:** Docker, Docker Compose.

Bring up the full stack — Postgres, Kafka, and the app — in one command:

```bash
docker-compose up --build
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

> Running the app on the host instead (`./mvnw spring-boot:run`) still
> works unchanged — just start the infra first with `docker-compose up -d
> postgres kafka`, since `application.properties` defaults to
> `localhost:5432`/`localhost:9092`.

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
