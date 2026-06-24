# Kafka Integration for JobFlowQ

## Context

JobFlowQ is a distributed job queue (Spring Boot 3.5.15, Java 17, Postgres). The
`Job` entity tracks generic queue tasks (`type`, `payload`, `status`, `priority`,
`retryCount`/`maxRetries`). There is currently no event bus — state changes are
only visible via polling the REST API or reading logs.

This spec adds Kafka as a side-channel event bus: whenever a job is created or
its status changes, a `JobApplicationEvent` is published to a
`job-application-events` topic. A logging consumer proves the round trip works.
Kafka publishing is best-effort and must never block or break the existing
queue/worker/REST behavior.

### Naming note

The originating request used job-application-tracker terminology
(`applicationId`, `companyName`). This codebase's domain is a generic job
*queue*, not a job-application tracker. Per user decision, these fields are
mapped directly onto the existing `Job` entity:

- `applicationId` = `Job.id` (no new identifier introduced)
- `companyName` = new nullable `Job.companyName` column (real schema field,
  not parsed out of the `payload` JSON blob)

## Goals

- Add `spring-kafka` to the project.
- Add a `companyName` column to `jobs` and surface it through `Job`,
  `JobRequest`, `JobResponse`.
- Define `JobApplicationEvent` and publish it via `JobEventProducer` to topic
  `job-application-events` whenever:
  - a job is created (`JobService.submitJob`) — `eventType = "CREATED"`
  - a job is cancelled (`JobService.cancelJob`) — `eventType = "STATUS_UPDATED"`
  - the worker claims a job (`PENDING → PROCESSING`) — `eventType = "STATUS_UPDATED"`
  - the worker completes a job (`PROCESSING → COMPLETED`) — `eventType = "STATUS_UPDATED"`
  - the worker retries a failed job (`PROCESSING → PENDING`) — `eventType = "STATUS_UPDATED"`
  - the worker dead-letters a job (`PROCESSING → DEAD`) — `eventType = "STATUS_UPDATED"`
- Add a logging `@KafkaListener` consumer for the same topic.
- Run a local Kafka broker via docker-compose (KRaft mode, no Zookeeper).
- Preserve all existing REST/worker/DB behavior. Kafka failures must not
  propagate as errors to callers or break job processing.

## Non-goals

- No new business logic tied to "job applications" as a separate domain.
- No transactional outbox / exactly-once delivery guarantees — publishing is
  fire-and-forget, logged on failure.
- No changes to `JobRepository`'s claim query or retry/dead-letter logic
  itself — only an event is added alongside each transition.
- No authentication/ACLs on the Kafka broker (local dev only, matches the
  existing unauthenticated local Postgres setup).

## Design

### 1. Dependency

`pom.xml`: add `org.springframework.kafka:spring-kafka`.

### 2. Schema change

`schema.sql`:
```sql
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS company_name VARCHAR(255);
```
Added after the existing `CREATE TABLE IF NOT EXISTS jobs` block, consistent
with the script's idempotent, re-runnable style (`spring.sql.init.mode=always`
re-executes it on every startup).

### 3. Entity / DTOs

- `Job.java`: add `private String companyName;` + getter/setter.
- `JobRequest.java`: add `companyName` field + getter/setter (optional, no
  `@NotBlank`).
- `JobResponse.java`: add `companyName` field + getter/setter.
- `JobService.submitJob` / `mapToResponse`: wire the new field through.

### 4. Event record

New file `dto/JobApplicationEvent.java`:
```java
public record JobApplicationEvent(
    Long applicationId,
    String companyName,
    JobStatus status,
    LocalDateTime updatedAt,
    String eventType
) {}
```

### 5. Kafka configuration

New file `config/KafkaConfig.java`:
- `ProducerFactory<String, JobApplicationEvent>` / `KafkaTemplate` beans using
  `StringSerializer` (key) + `JsonSerializer` (value).
- `ConsumerFactory<String, JobApplicationEvent>` /
  `ConcurrentKafkaListenerContainerFactory` beans using `StringDeserializer`
  (key) + `JsonDeserializer` (value), trusted packages restricted to
  `com.jobflowq.jobflowq.dto`.
- Bootstrap servers read from `spring.kafka.bootstrap-servers` property
  (`application.properties`), default `localhost:9092`.

### 6. Producer

New file `kafka/JobEventProducer.java`:
```java
@Component
public class JobEventProducer {
    private static final String TOPIC = "job-application-events";
    private final KafkaTemplate<String, JobApplicationEvent> kafkaTemplate;

    public void publish(JobApplicationEvent event) {
        try {
            kafkaTemplate.send(TOPIC, String.valueOf(event.applicationId()), event);
        } catch (Exception e) {
            logger.error("Failed to publish job event for applicationId={}", event.applicationId(), e);
        }
    }
}
```
Key = job id (string) → guarantees per-job ordering within a partition.
Catches synchronous send-setup exceptions; async send failures are logged via
a callback added to the returned `CompletableFuture`.

### 7. Consumer

New file `kafka/JobEventConsumer.java`:
```java
@Component
public class JobEventConsumer {
    @KafkaListener(topics = "job-application-events", groupId = "jobflowq-logger")
    public void onEvent(JobApplicationEvent event) {
        logger.info("Received job event: {}", event);
    }
}
```

### 8. Wiring into existing services

`JobService`:
- Constructor gains `JobEventProducer` dependency.
- `submitJob`: after `jobRepository.save(job)`, publish
  `new JobApplicationEvent(saved.getId(), saved.getCompanyName(), saved.getStatus(), saved.getUpdatedAt(), "CREATED")`.
- `cancelJob`: after `jobRepository.save(job)`, publish the same shape with
  `eventType = "STATUS_UPDATED"`.

`JobWorkerService`:
- Constructor gains `JobEventProducer` dependency.
- `processNextJob`: after setting status to `PROCESSING` and saving, publish
  `STATUS_UPDATED`. After setting `COMPLETED` and saving, publish
  `STATUS_UPDATED`.
- `handleFailure`: after `jobRepository.save(job)` (status now `PENDING` or
  `DEAD`), publish `STATUS_UPDATED`.

All publish calls happen after the DB save succeeds, so a Kafka failure never
rolls back or blocks the DB transaction — it's a side effect, not part of the
unit of work.

### 9. Configuration

`application.properties` additions:
```properties
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer
spring.kafka.consumer.group-id=jobflowq-logger
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer
spring.kafka.consumer.properties.spring.json.trusted.packages=com.jobflowq.jobflowq.dto
```

### 10. docker-compose

Add a `kafka` service in KRaft mode (single-node, no Zookeeper), exposing
`9092`, alongside the existing `postgres` service. Exact image/version chosen
during implementation (e.g. `apache/kafka:3.7.0`), consistent with the
project's existing pattern of one official image per service with minimal
config.

## Error handling

- Producer: catches exceptions around `send()`; logs and continues. Callers
  in `JobService`/`JobWorkerService` are unaffected by Kafka being down.
- Consumer: logging only; no retry/DLQ topic needed for a log-only consumer.
- Existing REST error handling (404/400/500 response bodies) is untouched.

## Testing

- `JobflowqApplicationTests` (context-load smoke test) must keep passing.
  Spring Kafka's producer/consumer beans don't connect eagerly at context
  startup, so this should hold without a running broker; verified during
  implementation.
- Manual smoke test: `docker-compose up` (now Postgres + Kafka), start the
  app, `POST /api/jobs`, confirm the `JobEventConsumer` logs a `CREATED`
  event, watch the worker logs for the subsequent `STATUS_UPDATED` events as
  the job is claimed and completed.
- Existing REST endpoint behavior (submit/get/list/cancel/metrics) re-verified
  manually to confirm no regression.

## Open items for implementation

- Exact Kafka docker image/tag (pick a maintained KRaft-mode image at
  implementation time).
- Whether `JsonSerializer`/`JsonDeserializer` need
  `spring.json.add.type.headers=false` to keep the wire format clean (decide
  during implementation based on consumer needs — only one consumer exists,
  so type headers are low-risk either way).
