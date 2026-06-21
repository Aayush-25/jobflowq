# JobFlowQ

A distributed job queue system built on PostgreSQL, using `SELECT ... FOR UPDATE SKIP LOCKED` for safe concurrent job processing across multiple worker instances.

## Architecture Overview

JobFlowQ uses PostgreSQL itself as the queue backend, relying on row-level locking instead of a separate message broker:

- Jobs are persisted in a single `jobs` table with `status`, `priority`, and `created_at` columns.
- Each worker polls on a fixed interval (every 500ms) and runs `findNextPendingJob()`, which executes:

  ```sql
  SELECT * FROM jobs
  WHERE status = 'PENDING'
  ORDER BY priority DESC, created_at ASC
  LIMIT 1
  FOR UPDATE SKIP LOCKED
  ```

- `FOR UPDATE` locks the selected row for the duration of the transaction. `SKIP LOCKED` tells Postgres to skip rows already locked by another worker's in-flight transaction rather than blocking on them.
- This means multiple worker instances can poll the same table concurrently with no double-processing and no explicit distributed lock manager — Postgres' MVCC handles contention.
- The query runs inside its own transaction (`Propagation.REQUIRES_NEW`), so claiming a job, marking it `PROCESSING`, and committing happens atomically and independently of the outer scheduled task.

## Features

- **Priority queue** — jobs are dequeued by `priority DESC, created_at ASC`, so higher-priority jobs always jump the line, with FIFO ordering within the same priority.
- **Retry logic** — failed jobs are re-queued as `PENDING` with an incremented `retry_count`, up to a configurable `max_retries` per job.
- **Dead-letter queue** — jobs that exhaust their retry budget are marked `DEAD` instead of being retried indefinitely or silently dropped.
- **REST API** — submit, fetch, list, and cancel jobs over HTTP.
- **Metrics endpoint** — live counts of jobs by status (`PENDING`, `PROCESSING`, `COMPLETED`, `FAILED`, `DEAD`).

## Tech Stack

- Java 17
- Spring Boot 3.5.15 (Web, Data JPA, Validation)
- PostgreSQL 15
- Docker / Docker Compose
- Maven

## Getting Started

**Prerequisites:** Docker, Java 17, Maven wrapper (bundled).

1. Start PostgreSQL:

   ```bash
   docker compose up -d
   ```

2. Run the application (schema is created automatically from `schema.sql` on startup):

   ```bash
   ./mvnw spring-boot:run
   ```

The API will be available at `http://localhost:8080`.

## API Endpoints

| Method | Path              | Description                                  |
|--------|--------------------|-----------------------------------------------|
| POST   | `/api/jobs`         | Submit a new job to the queue                |
| GET    | `/api/jobs/{id}`    | Fetch a single job by ID                     |
| GET    | `/api/jobs`         | List all jobs                                |
| DELETE | `/api/jobs/{id}`    | Cancel a `PENDING` job                       |
| GET    | `/api/metrics`      | Get queue metrics (counts by status)         |

**Example: submit a job**

```bash
curl -X POST http://localhost:8080/api/jobs \
  -H "Content-Type: application/json" \
  -d '{"type": "EMAIL", "payload": "{\"to\":\"user@example.com\"}", "priority": 10, "maxRetries": 3}'
```

## How It Works

A scheduled task fires every 500ms on each worker and attempts to claim the highest-priority `PENDING` job via the `SKIP LOCKED` query, immediately marking it `PROCESSING` and tagging it with the worker's ID. The job's work is then executed; on success it transitions to `COMPLETED` with a `completed_at` timestamp, and on failure its `retry_count` is incremented and it returns to `PENDING` for another attempt. Once `retry_count` reaches `max_retries`, the job is moved to `DEAD` instead of being retried again, acting as a dead-letter queue. Because claiming, status updates, and retries all happen through ordinary SQL transactions against the same table, the system requires no external broker — Postgres provides both the durable store and the concurrency control.
