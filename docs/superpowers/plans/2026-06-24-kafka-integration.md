# Kafka Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Kafka event publishing to JobFlowQ — fire `JobApplicationEvent`s on job creation, cancellation, and every worker-driven status transition, without changing any existing REST/worker/DB behavior.

**Architecture:** A new `JobApplicationEvent` record is published via `JobEventProducer` (wrapping a `KafkaTemplate`) to the `job-application-events` topic from `JobService` (create/cancel) and `JobWorkerService` (claim/complete/retry/dead-letter). A logging `JobEventConsumer` proves the round trip. `KafkaConfig` defines the producer/consumer beans explicitly. A new `companyName` column is added to `jobs` and threaded through `Job`/`JobRequest`/`JobResponse`. A local Kafka broker (KRaft mode) is added to `docker-compose.yml`.

**Tech Stack:** Spring Boot 3.5.15, Java 17, spring-kafka, Postgres, JUnit 5 + Mockito (already in `spring-boot-starter-test`).

## Global Constraints

- Topic name is exactly `job-application-events` — used verbatim in producer, consumer, and tests.
- `applicationId` = `Job.id` (no new identifier). `companyName` = new nullable `Job.companyName` field (real column, not parsed from `payload`).
- `eventType` is the literal string `"CREATED"` (on submit) or `"STATUS_UPDATED"` (everywhere else) — no other values.
- Publishing is best-effort: a Kafka failure must never throw out of `JobService`/`JobWorkerService` methods or roll back a DB transaction.
- No changes to `JobRepository`'s claim query, retry/dead-letter thresholds, or any existing REST response shape/status codes beyond adding the `companyName` field.
- `spring.kafka.bootstrap-servers` defaults to `localhost:9092`.
- No Lombok (matches existing codebase convention).

---

### Task 1: JobApplicationEvent record + spring-kafka dependency

**Files:**
- Modify: `pom.xml`
- Create: `src/main/java/com/jobflowq/jobflowq/dto/JobApplicationEvent.java`

**Interfaces:**
- Produces: `record JobApplicationEvent(Long applicationId, String companyName, JobStatus status, LocalDateTime updatedAt, String eventType)` — used by every later task.

- [ ] **Step 1: Add the spring-kafka dependency**

In `pom.xml`, inside the existing `<dependencies>` block, add (right after the `spring-boot-starter-validation` dependency, before `spring-boot-starter-web`):

```xml
		<dependency>
			<groupId>org.springframework.kafka</groupId>
			<artifactId>spring-kafka</artifactId>
		</dependency>
```

- [ ] **Step 2: Create the event record**

Create `src/main/java/com/jobflowq/jobflowq/dto/JobApplicationEvent.java`:

```java
package com.jobflowq.jobflowq.dto;

import com.jobflowq.jobflowq.model.JobStatus;

import java.time.LocalDateTime;

public record JobApplicationEvent(
        Long applicationId,
        String companyName,
        JobStatus status,
        LocalDateTime updatedAt,
        String eventType
) {
}
```

- [ ] **Step 3: Verify the project compiles**

Run: `./mvnw -q compile`
Expected: `BUILD SUCCESS`, no errors.

- [ ] **Step 4: Commit**

```bash
git add pom.xml src/main/java/com/jobflowq/jobflowq/dto/JobApplicationEvent.java
git commit -m "feat: add spring-kafka dependency and JobApplicationEvent record"
```

---

### Task 2: KafkaConfig (producer + consumer beans) + bootstrap-servers config

**Files:**
- Create: `src/main/java/com/jobflowq/jobflowq/config/KafkaConfig.java`
- Modify: `src/main/resources/application.properties`
- Test: existing `src/test/java/com/jobflowq/jobflowq/JobflowqApplicationTests.java` (no changes needed — used to verify context still loads)

**Interfaces:**
- Consumes: `JobApplicationEvent` from Task 1.
- Produces: `KafkaTemplate<String, JobApplicationEvent>` bean (consumed by Task 3's `JobEventProducer`), `ConcurrentKafkaListenerContainerFactory<String, JobApplicationEvent>` bean (consumed implicitly by `@KafkaListener` in Task 4 via Spring Boot autoconfiguration of listener containers).

- [ ] **Step 1: Add bootstrap-servers property**

In `src/main/resources/application.properties`, append:

```properties

spring.kafka.bootstrap-servers=localhost:9092
```

- [ ] **Step 2: Write KafkaConfig**

Create `src/main/java/com/jobflowq/jobflowq/config/KafkaConfig.java`:

```java
package com.jobflowq.jobflowq.config;

import com.jobflowq.jobflowq.dto.JobApplicationEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    public static final String CONSUMER_GROUP_ID = "jobflowq-logger";

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, JobApplicationEvent> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, JobApplicationEvent> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ConsumerFactory<String, JobApplicationEvent> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "com.jobflowq.jobflowq.dto");
        config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, JobApplicationEvent.class.getName());
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, JobApplicationEvent> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, JobApplicationEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
}
```

- [ ] **Step 3: Verify the existing context-load test still passes without a running broker**

Run: `./mvnw -q test -Dtest=JobflowqApplicationTests`
Expected: `BUILD SUCCESS`. Spring Kafka's `ProducerFactory`/`ConsumerFactory` beans don't connect eagerly, so this must pass with no Kafka broker running locally. If it fails with a connection error at context startup, stop and report — that means the design assumption about lazy connection doesn't hold for this Spring Kafka version, and bootstrap-servers configuration needs adjusting before continuing.

- [ ] **Step 4: Commit**

```bash
git add src/main/java/com/jobflowq/jobflowq/config/KafkaConfig.java src/main/resources/application.properties
git commit -m "feat: add KafkaConfig with producer and consumer beans"
```

---

### Task 3: JobEventProducer

**Files:**
- Create: `src/main/java/com/jobflowq/jobflowq/kafka/JobEventProducer.java`
- Test: `src/test/java/com/jobflowq/jobflowq/kafka/JobEventProducerTest.java`

**Interfaces:**
- Consumes: `KafkaTemplate<String, JobApplicationEvent>` bean from Task 2.
- Produces: `JobEventProducer.TOPIC` constant (`"job-application-events"`) and `void publish(JobApplicationEvent event)` — used by Task 5 (`JobService`) and Task 6 (`JobWorkerService`).

- [ ] **Step 1: Write the failing tests**

Create `src/test/java/com/jobflowq/jobflowq/kafka/JobEventProducerTest.java`:

```java
package com.jobflowq.jobflowq.kafka;

import com.jobflowq.jobflowq.dto.JobApplicationEvent;
import com.jobflowq.jobflowq.model.JobStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class JobEventProducerTest {

    @Mock
    private KafkaTemplate<String, JobApplicationEvent> kafkaTemplate;

    @Test
    void publishSendsEventToTopicWithJobIdAsKey() {
        JobEventProducer producer = new JobEventProducer(kafkaTemplate);
        JobApplicationEvent event = new JobApplicationEvent(42L, "Acme Corp", JobStatus.PENDING, LocalDateTime.now(), "CREATED");
        when(kafkaTemplate.send(eq(JobEventProducer.TOPIC), eq("42"), eq(event)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        producer.publish(event);

        verify(kafkaTemplate).send(JobEventProducer.TOPIC, "42", event);
    }

    @Test
    void publishDoesNotThrowWhenSendFailsAsynchronously() {
        JobEventProducer producer = new JobEventProducer(kafkaTemplate);
        JobApplicationEvent event = new JobApplicationEvent(7L, "Globex", JobStatus.FAILED, LocalDateTime.now(), "STATUS_UPDATED");
        CompletableFuture<SendResult<String, JobApplicationEvent>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("broker down"));
        when(kafkaTemplate.send(eq(JobEventProducer.TOPIC), eq("7"), eq(event))).thenReturn(failed);

        producer.publish(event);

        verify(kafkaTemplate).send(JobEventProducer.TOPIC, "7", event);
    }

    @Test
    void publishDoesNotThrowWhenSendThrowsSynchronously() {
        JobEventProducer producer = new JobEventProducer(kafkaTemplate);
        JobApplicationEvent event = new JobApplicationEvent(9L, "Initech", JobStatus.DEAD, LocalDateTime.now(), "STATUS_UPDATED");
        when(kafkaTemplate.send(eq(JobEventProducer.TOPIC), eq("9"), eq(event)))
                .thenThrow(new RuntimeException("serialization error"));

        producer.publish(event);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./mvnw -q test -Dtest=JobEventProducerTest`
Expected: FAIL — compile error, `JobEventProducer` class does not exist yet.

- [ ] **Step 3: Write the implementation**

Create `src/main/java/com/jobflowq/jobflowq/kafka/JobEventProducer.java`:

```java
package com.jobflowq.jobflowq.kafka;

import com.jobflowq.jobflowq.dto.JobApplicationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class JobEventProducer {

    public static final String TOPIC = "job-application-events";

    private static final Logger logger = LoggerFactory.getLogger(JobEventProducer.class);

    private final KafkaTemplate<String, JobApplicationEvent> kafkaTemplate;

    public JobEventProducer(KafkaTemplate<String, JobApplicationEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(JobApplicationEvent event) {
        try {
            kafkaTemplate.send(TOPIC, String.valueOf(event.applicationId()), event)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            logger.error("Failed to publish job event for applicationId={}", event.applicationId(), ex);
                        }
                    });
        } catch (Exception e) {
            logger.error("Failed to publish job event for applicationId={}", event.applicationId(), e);
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./mvnw -q test -Dtest=JobEventProducerTest`
Expected: `BUILD SUCCESS`, 3 tests passed.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/jobflowq/jobflowq/kafka/JobEventProducer.java src/test/java/com/jobflowq/jobflowq/kafka/JobEventProducerTest.java
git commit -m "feat: add JobEventProducer publishing to job-application-events topic"
```

---

### Task 4: JobEventConsumer

**Files:**
- Create: `src/main/java/com/jobflowq/jobflowq/kafka/JobEventConsumer.java`
- Test: `src/test/java/com/jobflowq/jobflowq/kafka/JobEventConsumerTest.java`

**Interfaces:**
- Consumes: `JobEventProducer.TOPIC` constant from Task 3, `JobApplicationEvent` from Task 1.
- Produces: nothing consumed by later tasks — this is a terminal logging sink.

- [ ] **Step 1: Write the failing test**

Create `src/test/java/com/jobflowq/jobflowq/kafka/JobEventConsumerTest.java`:

```java
package com.jobflowq.jobflowq.kafka;

import com.jobflowq.jobflowq.dto.JobApplicationEvent;
import com.jobflowq.jobflowq.model.JobStatus;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class JobEventConsumerTest {

    @Test
    void onEventLogsWithoutThrowing() {
        JobEventConsumer consumer = new JobEventConsumer();
        JobApplicationEvent event = new JobApplicationEvent(1L, "Acme Corp", JobStatus.PENDING, LocalDateTime.now(), "CREATED");

        assertDoesNotThrow(() -> consumer.onEvent(event));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./mvnw -q test -Dtest=JobEventConsumerTest`
Expected: FAIL — compile error, `JobEventConsumer` class does not exist yet.

- [ ] **Step 3: Write the implementation**

Create `src/main/java/com/jobflowq/jobflowq/kafka/JobEventConsumer.java`:

```java
package com.jobflowq.jobflowq.kafka;

import com.jobflowq.jobflowq.dto.JobApplicationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class JobEventConsumer {

    private static final Logger logger = LoggerFactory.getLogger(JobEventConsumer.class);

    @KafkaListener(topics = JobEventProducer.TOPIC, groupId = "jobflowq-logger")
    public void onEvent(JobApplicationEvent event) {
        logger.info("Received job event: {}", event);
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./mvnw -q test -Dtest=JobEventConsumerTest`
Expected: `BUILD SUCCESS`, 1 test passed.

- [ ] **Step 5: Verify full context still loads (consumer beans wired correctly)**

Run: `./mvnw -q test -Dtest=JobflowqApplicationTests`
Expected: `BUILD SUCCESS`.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/jobflowq/jobflowq/kafka/JobEventConsumer.java src/test/java/com/jobflowq/jobflowq/kafka/JobEventConsumerTest.java
git commit -m "feat: add logging JobEventConsumer for job-application-events topic"
```

---

### Task 5: companyName field + JobService event wiring

**Files:**
- Modify: `src/main/resources/schema.sql`
- Modify: `src/main/java/com/jobflowq/jobflowq/model/Job.java`
- Modify: `src/main/java/com/jobflowq/jobflowq/dto/JobRequest.java`
- Modify: `src/main/java/com/jobflowq/jobflowq/dto/JobResponse.java`
- Modify: `src/main/java/com/jobflowq/jobflowq/service/JobService.java`
- Test: `src/test/java/com/jobflowq/jobflowq/service/JobServiceTest.java` (new)

**Interfaces:**
- Consumes: `JobEventProducer.publish(JobApplicationEvent)` from Task 3.
- Produces: `Job.getCompanyName()`/`setCompanyName(String)` — used by Task 6.

- [ ] **Step 1: Add the schema column**

In `src/main/resources/schema.sql`, append after the existing `CREATE INDEX` statement:

```sql

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS company_name VARCHAR(255);
```

- [ ] **Step 2: Add companyName to the Job entity**

In `src/main/java/com/jobflowq/jobflowq/model/Job.java`, add the field after `private String type;`:

```java
    private String companyName;
```

Add the getter/setter after `getType`/`setType`:

```java
    public String getCompanyName() { return companyName; }
    public void setCompanyName(String companyName) { this.companyName = companyName; }
```

- [ ] **Step 3: Add companyName to JobRequest**

In `src/main/java/com/jobflowq/jobflowq/dto/JobRequest.java`, add the field after the `type` field:

```java
    private String companyName;
```

Add the getter/setter:

```java
    public String getCompanyName() { return companyName; }
    public void setCompanyName(String companyName) { this.companyName = companyName; }
```

- [ ] **Step 4: Add companyName to JobResponse**

In `src/main/java/com/jobflowq/jobflowq/dto/JobResponse.java`, add the field after `type`:

```java
    private String companyName;
```

Add the getter/setter:

```java
    public String getCompanyName() { return companyName; }
    public void setCompanyName(String companyName) { this.companyName = companyName; }
```

- [ ] **Step 5: Write the failing tests for JobService**

Create `src/test/java/com/jobflowq/jobflowq/service/JobServiceTest.java`:

```java
package com.jobflowq.jobflowq.service;

import com.jobflowq.jobflowq.dto.JobApplicationEvent;
import com.jobflowq.jobflowq.dto.JobRequest;
import com.jobflowq.jobflowq.dto.JobResponse;
import com.jobflowq.jobflowq.kafka.JobEventProducer;
import com.jobflowq.jobflowq.model.Job;
import com.jobflowq.jobflowq.model.JobStatus;
import com.jobflowq.jobflowq.repository.JobRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class JobServiceTest {

    @Mock
    private JobRepository jobRepository;

    @Mock
    private JobEventProducer jobEventProducer;

    private JobService newService() {
        return new JobService(jobRepository, jobEventProducer);
    }

    @Test
    void submitJobSavesCompanyNameAndPublishesCreatedEvent() {
        JobService jobService = newService();
        when(jobRepository.save(any(Job.class))).thenAnswer(invocation -> {
            Job job = invocation.getArgument(0);
            job.setId(100L);
            return job;
        });

        JobRequest request = new JobRequest();
        request.setType("EMAIL");
        request.setCompanyName("Acme Corp");

        JobResponse response = jobService.submitJob(request);

        assertEquals("Acme Corp", response.getCompanyName());
        assertEquals(100L, response.getId());

        ArgumentCaptor<JobApplicationEvent> captor = ArgumentCaptor.forClass(JobApplicationEvent.class);
        verify(jobEventProducer).publish(captor.capture());
        JobApplicationEvent event = captor.getValue();
        assertEquals(100L, event.applicationId());
        assertEquals("Acme Corp", event.companyName());
        assertEquals(JobStatus.PENDING, event.status());
        assertEquals("CREATED", event.eventType());
    }

    @Test
    void cancelJobWhenPendingPublishesStatusUpdatedEvent() {
        JobService jobService = newService();
        Job existing = new Job();
        existing.setId(5L);
        existing.setType("REPORT");
        existing.setCompanyName("Globex");
        existing.setStatus(JobStatus.PENDING);
        when(jobRepository.findById(5L)).thenReturn(Optional.of(existing));
        when(jobRepository.save(any(Job.class))).thenAnswer(invocation -> invocation.getArgument(0));

        JobResponse response = jobService.cancelJob(5L);

        assertEquals(JobStatus.FAILED, response.getStatus());

        ArgumentCaptor<JobApplicationEvent> captor = ArgumentCaptor.forClass(JobApplicationEvent.class);
        verify(jobEventProducer).publish(captor.capture());
        JobApplicationEvent event = captor.getValue();
        assertEquals(5L, event.applicationId());
        assertEquals("Globex", event.companyName());
        assertEquals(JobStatus.FAILED, event.status());
        assertEquals("STATUS_UPDATED", event.eventType());
    }

    @Test
    void cancelJobWhenNotPendingDoesNotPublishEvent() {
        JobService jobService = newService();
        Job existing = new Job();
        existing.setId(6L);
        existing.setStatus(JobStatus.PROCESSING);
        when(jobRepository.findById(6L)).thenReturn(Optional.of(existing));

        assertThrows(RuntimeException.class, () -> jobService.cancelJob(6L));

        verify(jobEventProducer, never()).publish(any());
    }
}
```

- [ ] **Step 6: Run tests to verify they fail**

Run: `./mvnw -q test -Dtest=JobServiceTest`
Expected: FAIL — compile error, `JobService` constructor doesn't accept a `JobEventProducer` yet, and `JobRequest`/`JobResponse` don't yet have `companyName` (steps 1-4 above already added those DTO fields, so the only remaining compile error should be the `JobService` constructor signature).

- [ ] **Step 7: Update JobService**

Replace the full contents of `src/main/java/com/jobflowq/jobflowq/service/JobService.java`:

```java
package com.jobflowq.jobflowq.service;

import com.jobflowq.jobflowq.dto.JobApplicationEvent;
import com.jobflowq.jobflowq.dto.JobRequest;
import com.jobflowq.jobflowq.dto.JobResponse;
import com.jobflowq.jobflowq.dto.QueueMetrics;
import com.jobflowq.jobflowq.kafka.JobEventProducer;
import com.jobflowq.jobflowq.model.Job;
import com.jobflowq.jobflowq.model.JobStatus;
import com.jobflowq.jobflowq.repository.JobRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Service
public class JobService {

    private static final Logger logger = LoggerFactory.getLogger(JobService.class);

    private final JobRepository jobRepository;
    private final JobEventProducer jobEventProducer;

    public JobService(JobRepository jobRepository, JobEventProducer jobEventProducer) {
        this.jobRepository = jobRepository;
        this.jobEventProducer = jobEventProducer;
    }

    @Transactional
    public JobResponse submitJob(JobRequest request) {
        Job job = new Job();
        job.setType(request.getType());
        job.setCompanyName(request.getCompanyName());
        job.setPayload(request.getPayload() != null ? request.getPayload() : "{}");
        job.setPriority(request.getPriority() != null ? request.getPriority() : 5);
        job.setMaxRetries(request.getMaxRetries() != null ? request.getMaxRetries() : 3);
        job.setStatus(JobStatus.PENDING);

        Job saved = jobRepository.save(job);
        logger.info("Submitted job id={} type={}", saved.getId(), saved.getType());
        publishEvent(saved, "CREATED");
        return mapToResponse(saved);
    }

    @Transactional(readOnly = true)
    public JobResponse getJob(Long id) {
        Job job = jobRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Job not found with id: " + id));
        return mapToResponse(job);
    }

    @Transactional(readOnly = true)
    public java.util.List<JobResponse> getAllJobs() {
        return jobRepository.findAll().stream()
                .map(this::mapToResponse)
                .toList();
    }

    @Transactional
    public JobResponse cancelJob(Long id) {
        Job job = jobRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Job not found with id: " + id));

        if (job.getStatus() != JobStatus.PENDING) {
            throw new RuntimeException("Cannot cancel job with status: " + job.getStatus());
        }

        job.setStatus(JobStatus.FAILED);
        job.setErrorMessage("Cancelled by user");
        job.setUpdatedAt(LocalDateTime.now());

        Job saved = jobRepository.save(job);
        logger.info("Cancelled job id={}", saved.getId());
        publishEvent(saved, "STATUS_UPDATED");
        return mapToResponse(saved);
    }

    @Transactional(readOnly = true)
    public QueueMetrics getMetrics() {
        long pending = jobRepository.countByStatus(JobStatus.PENDING);
        long processing = jobRepository.countByStatus(JobStatus.PROCESSING);
        long completed = jobRepository.countByStatus(JobStatus.COMPLETED);
        long failed = jobRepository.countByStatus(JobStatus.FAILED);
        long dead = jobRepository.countByStatus(JobStatus.DEAD);
        long totalProcessed = completed + failed + dead;

        return new QueueMetrics(pending, processing, completed, failed, dead, totalProcessed);
    }

    private void publishEvent(Job job, String eventType) {
        jobEventProducer.publish(new JobApplicationEvent(
                job.getId(),
                job.getCompanyName(),
                job.getStatus(),
                job.getUpdatedAt(),
                eventType
        ));
    }

    private JobResponse mapToResponse(Job job) {
        JobResponse response = new JobResponse();
        response.setId(job.getId());
        response.setType(job.getType());
        response.setCompanyName(job.getCompanyName());
        response.setPayload(job.getPayload());
        response.setStatus(job.getStatus());
        response.setPriority(job.getPriority());
        response.setRetryCount(job.getRetryCount());
        response.setMaxRetries(job.getMaxRetries());
        response.setWorkerId(job.getWorkerId());
        response.setErrorMessage(job.getErrorMessage());
        response.setCreatedAt(job.getCreatedAt());
        response.setUpdatedAt(job.getUpdatedAt());
        response.setCompletedAt(job.getCompletedAt());
        return response;
    }
}
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `./mvnw -q test -Dtest=JobServiceTest`
Expected: `BUILD SUCCESS`, 3 tests passed.

- [ ] **Step 9: Run full test suite to confirm no regressions**

Run: `./mvnw -q test`
Expected: `BUILD SUCCESS`, all tests pass (including `JobflowqApplicationTests`, `JobEventProducerTest`, `JobEventConsumerTest`, `JobServiceTest`).

- [ ] **Step 10: Commit**

```bash
git add src/main/resources/schema.sql src/main/java/com/jobflowq/jobflowq/model/Job.java src/main/java/com/jobflowq/jobflowq/dto/JobRequest.java src/main/java/com/jobflowq/jobflowq/dto/JobResponse.java src/main/java/com/jobflowq/jobflowq/service/JobService.java src/test/java/com/jobflowq/jobflowq/service/JobServiceTest.java
git commit -m "feat: add companyName field and publish job events from JobService"
```

---

### Task 6: JobWorkerService event wiring

**Files:**
- Modify: `src/main/java/com/jobflowq/jobflowq/service/JobWorkerService.java`
- Test: `src/test/java/com/jobflowq/jobflowq/service/JobWorkerServiceTest.java` (new)

**Interfaces:**
- Consumes: `JobEventProducer.publish(JobApplicationEvent)` from Task 3, `Job.getCompanyName()` from Task 5.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Write the failing tests**

Create `src/test/java/com/jobflowq/jobflowq/service/JobWorkerServiceTest.java`:

```java
package com.jobflowq.jobflowq.service;

import com.jobflowq.jobflowq.dto.JobApplicationEvent;
import com.jobflowq.jobflowq.kafka.JobEventProducer;
import com.jobflowq.jobflowq.model.Job;
import com.jobflowq.jobflowq.model.JobStatus;
import com.jobflowq.jobflowq.repository.JobRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class JobWorkerServiceTest {

    @Mock
    private JobRepository jobRepository;

    @Mock
    private JobEventProducer jobEventProducer;

    private JobWorkerService newWorker() {
        return new JobWorkerService(jobRepository, jobEventProducer);
    }

    @Test
    void processNextJobPublishesClaimAndCompleteEvents() {
        JobWorkerService worker = newWorker();
        Job job = new Job();
        job.setId(1L);
        job.setType("EMAIL");
        job.setCompanyName("Acme Corp");
        job.setStatus(JobStatus.PENDING);
        when(jobRepository.findNextPendingJob()).thenReturn(Optional.of(job));
        when(jobRepository.save(any(Job.class))).thenAnswer(invocation -> invocation.getArgument(0));

        worker.processNextJob();

        ArgumentCaptor<JobApplicationEvent> captor = ArgumentCaptor.forClass(JobApplicationEvent.class);
        verify(jobEventProducer, times(2)).publish(captor.capture());
        List<JobApplicationEvent> events = captor.getAllValues();
        assertEquals(JobStatus.PROCESSING, events.get(0).status());
        assertEquals("STATUS_UPDATED", events.get(0).eventType());
        assertEquals(JobStatus.COMPLETED, events.get(1).status());
        assertEquals("STATUS_UPDATED", events.get(1).eventType());
    }

    @Test
    void processNextJobPublishesNothingWhenQueueEmpty() {
        JobWorkerService worker = newWorker();
        when(jobRepository.findNextPendingJob()).thenReturn(Optional.empty());

        worker.processNextJob();

        verify(jobEventProducer, never()).publish(any());
    }

    @Test
    void handleFailureBelowMaxRetriesPublishesPendingStatus() {
        JobWorkerService worker = newWorker();
        Job job = new Job();
        job.setId(2L);
        job.setCompanyName("Globex");
        job.setRetryCount(0);
        job.setMaxRetries(3);
        when(jobRepository.save(any(Job.class))).thenAnswer(invocation -> invocation.getArgument(0));

        worker.handleFailure(job, new RuntimeException("boom"));

        ArgumentCaptor<JobApplicationEvent> captor = ArgumentCaptor.forClass(JobApplicationEvent.class);
        verify(jobEventProducer).publish(captor.capture());
        assertEquals(JobStatus.PENDING, captor.getValue().status());
        assertEquals(JobStatus.PENDING, job.getStatus());
    }

    @Test
    void handleFailureAtMaxRetriesPublishesDeadStatus() {
        JobWorkerService worker = newWorker();
        Job job = new Job();
        job.setId(3L);
        job.setCompanyName("Initech");
        job.setRetryCount(2);
        job.setMaxRetries(3);
        when(jobRepository.save(any(Job.class))).thenAnswer(invocation -> invocation.getArgument(0));

        worker.handleFailure(job, new RuntimeException("boom"));

        ArgumentCaptor<JobApplicationEvent> captor = ArgumentCaptor.forClass(JobApplicationEvent.class);
        verify(jobEventProducer).publish(captor.capture());
        assertEquals(JobStatus.DEAD, captor.getValue().status());
        assertEquals(JobStatus.DEAD, job.getStatus());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./mvnw -q test -Dtest=JobWorkerServiceTest`
Expected: FAIL — compile errors: `JobWorkerService` constructor doesn't accept `JobEventProducer`, and `handleFailure` is `private` (not visible to the test class).

- [ ] **Step 3: Update JobWorkerService**

Replace the full contents of `src/main/java/com/jobflowq/jobflowq/service/JobWorkerService.java`:

```java
package com.jobflowq.jobflowq.service;

import com.jobflowq.jobflowq.dto.JobApplicationEvent;
import com.jobflowq.jobflowq.kafka.JobEventProducer;
import com.jobflowq.jobflowq.model.Job;
import com.jobflowq.jobflowq.model.JobStatus;
import com.jobflowq.jobflowq.repository.JobRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Optional;

@Service
public class JobWorkerService {

    private static final Logger logger = LoggerFactory.getLogger(JobWorkerService.class);

    private final JobRepository jobRepository;
    private final JobEventProducer jobEventProducer;
    private final String workerId;

    public JobWorkerService(JobRepository jobRepository, JobEventProducer jobEventProducer) {
        this.jobRepository = jobRepository;
        this.jobEventProducer = jobEventProducer;
        String envWorkerId = System.getenv("WORKER_ID");
        this.workerId = (envWorkerId != null && !envWorkerId.isBlank()) ? envWorkerId : "worker-1";
    }

    @Scheduled(fixedDelay = 500)
    public void pollAndProcess() {
        try {
            processNextJob();
        } catch (Exception e) {
            logger.error("Unexpected error while polling for jobs on worker={}", workerId, e);
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void processNextJob() {
        Optional<Job> jobOptional = jobRepository.findNextPendingJob();
        if (jobOptional.isEmpty()) {
            return;
        }

        Job job = jobOptional.get();
        job.setStatus(JobStatus.PROCESSING);
        job.setWorkerId(workerId);
        job.setUpdatedAt(LocalDateTime.now());
        jobRepository.save(job);
        publishStatusUpdate(job);

        long startTime = System.currentTimeMillis();
        try {
            simulateProcessing(job.getType());

            job.setStatus(JobStatus.COMPLETED);
            job.setCompletedAt(LocalDateTime.now());
            job.setUpdatedAt(LocalDateTime.now());
            jobRepository.save(job);
            publishStatusUpdate(job);

            long timeTaken = System.currentTimeMillis() - startTime;
            logger.info("Job id={} type={} status={} worker={} timeTakenMs={}",
                    job.getId(), job.getType(), job.getStatus(), workerId, timeTaken);
        } catch (Exception e) {
            handleFailure(job, e);

            long timeTaken = System.currentTimeMillis() - startTime;
            logger.error("Job id={} type={} status={} worker={} timeTakenMs={} error={}",
                    job.getId(), job.getType(), job.getStatus(), workerId, timeTaken, e.getMessage());
        }
    }

    void handleFailure(Job job, Exception e) {
        int retryCount = job.getRetryCount() + 1;
        job.setRetryCount(retryCount);
        job.setErrorMessage(e.getMessage());
        job.setUpdatedAt(LocalDateTime.now());

        if (retryCount >= job.getMaxRetries()) {
            job.setStatus(JobStatus.DEAD);
        } else {
            job.setStatus(JobStatus.PENDING);
        }

        jobRepository.save(job);
        publishStatusUpdate(job);
    }

    private void publishStatusUpdate(Job job) {
        jobEventProducer.publish(new JobApplicationEvent(
                job.getId(),
                job.getCompanyName(),
                job.getStatus(),
                job.getUpdatedAt(),
                "STATUS_UPDATED"
        ));
    }

    private void simulateProcessing(String type) throws InterruptedException {
        long sleepMs;
        if (type == null) {
            sleepMs = 300;
        } else {
            switch (type.toUpperCase()) {
                case "EMAIL" -> sleepMs = 200;
                case "REPORT" -> sleepMs = 800;
                case "EXPORT" -> sleepMs = 1500;
                default -> sleepMs = 300;
            }
        }
        Thread.sleep(sleepMs);
    }
}
```

Note: `handleFailure` changed from `private` to package-private (default access) solely so the test in the same package can exercise it directly — there is no other path to trigger the failure branch in a unit test, since `simulateProcessing` only sleeps and never throws.

- [ ] **Step 4: Run tests to verify they pass**

Run: `./mvnw -q test -Dtest=JobWorkerServiceTest`
Expected: `BUILD SUCCESS`, 4 tests passed.

- [ ] **Step 5: Run full test suite to confirm no regressions**

Run: `./mvnw -q test`
Expected: `BUILD SUCCESS`, all tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/jobflowq/jobflowq/service/JobWorkerService.java src/test/java/com/jobflowq/jobflowq/service/JobWorkerServiceTest.java
git commit -m "feat: publish job events from JobWorkerService on every status transition"
```

---

### Task 7: docker-compose Kafka broker

**Files:**
- Modify: `docker-compose.yml`

**Interfaces:**
- Produces: a running Kafka broker on `localhost:9092` — required for Task 8's manual end-to-end verification.

- [ ] **Step 1: Add the kafka service**

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

  kafka:
    image: apache/kafka:3.7.0
    container_name: jobflowq-kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

volumes:
  postgres_data:
```

- [ ] **Step 2: Verify the broker starts**

Run: `docker-compose up -d`
Run: `docker-compose ps`
Expected: both `jobflowq-db` and `jobflowq-kafka` show state `Up`/`running`.

Run: `docker-compose logs kafka | tail -20`
Expected: log lines indicating the broker started (e.g. containing `Kafka Server started`), no repeated `ERROR` lines.

- [ ] **Step 3: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: add Kafka broker (KRaft mode) to docker-compose"
```

Leave the stack running (`docker-compose up -d`) — Task 8 uses it directly.

---

### Task 8: End-to-end verification

**Files:** none (verification only)

**Interfaces:**
- Consumes: everything from Tasks 1-7.

- [ ] **Step 1: Run the full automated test suite**

Run: `./mvnw -q test`
Expected: `BUILD SUCCESS`, all tests pass (`JobflowqApplicationTests`, `JobEventProducerTest`, `JobEventConsumerTest`, `JobServiceTest`, `JobWorkerServiceTest`).

- [ ] **Step 2: Confirm Postgres + Kafka are up**

Run: `docker-compose ps`
Expected: `jobflowq-db` and `jobflowq-kafka` both `Up`.

- [ ] **Step 3: Start the application**

Run: `./mvnw spring-boot:run` (in a separate terminal/background process)
Expected: startup logs show no errors, ending with `Started JobflowqApplication`.

- [ ] **Step 4: Submit a job and confirm companyName round-trips**

Run:
```bash
curl -s -X POST http://localhost:8080/api/jobs \
  -H "Content-Type: application/json" \
  -d '{"type":"EMAIL","companyName":"Acme Corp"}'
```
Expected: HTTP 201, JSON body includes `"companyName":"Acme Corp"` and `"status":"PENDING"`. Note the returned `id`.

- [ ] **Step 5: Confirm the CREATED event was consumed**

Check the application's console/log output for a line containing:
```
Received job event: JobApplicationEvent[applicationId=<id>, companyName=Acme Corp, status=PENDING, ..., eventType=CREATED]
```

- [ ] **Step 6: Confirm worker transition events appear**

Within ~1 second, expect two more consumer log lines for the same `id`:
```
Received job event: JobApplicationEvent[applicationId=<id>, ..., status=PROCESSING, ..., eventType=STATUS_UPDATED]
Received job event: JobApplicationEvent[applicationId=<id>, ..., status=COMPLETED, ..., eventType=STATUS_UPDATED]
```

- [ ] **Step 7: Confirm existing endpoints still work**

Run:
```bash
curl -s http://localhost:8080/api/jobs/<id>
curl -s http://localhost:8080/api/jobs
curl -s http://localhost:8080/api/metrics
```
Expected: all return 200 with the job/list/metrics data as before, now including `companyName` in job objects.

- [ ] **Step 8: Confirm cancel still works and fires an event**

Submit a second job, then immediately cancel it before the worker claims it:
```bash
curl -s -X POST http://localhost:8080/api/jobs -H "Content-Type: application/json" -d '{"type":"EXPORT","companyName":"Globex"}'
curl -s -X DELETE http://localhost:8080/api/jobs/<new-id>
```
Expected: DELETE returns 200 with `"status":"FAILED"`, and a `STATUS_UPDATED` / `eventType=STATUS_UPDATED` consumer log line appears for that job. (If the worker claims it first due to timing, status will already be `PROCESSING` and the DELETE will return 400 "Cannot cancel job with status: PROCESSING" — this is existing, unchanged behavior, not a regression.)

- [ ] **Step 9: Tear down**

Stop the running application, then:
```bash
docker-compose down
```
Expected: containers stop cleanly.

- [ ] **Step 10: Report results**

Summarize pass/fail for each step above. If any step fails, do not mark the plan complete — report the failure and stop for review.
