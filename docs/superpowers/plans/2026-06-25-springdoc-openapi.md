# SpringDoc OpenAPI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add SpringDoc OpenAPI annotations and a Swagger UI to JobFlowQ's existing REST API, with zero changes to existing logic.

**Architecture:** Add the `springdoc-openapi-starter-webmvc-ui` dependency and a config bean that supplies API metadata; annotate the two existing controllers and two existing DTOs with documentation annotations only — no method bodies or field types change.

**Tech Stack:** Spring Boot 3.5.15, Java 17, `org.springdoc:springdoc-openapi-starter-webmvc-ui:2.8.17`.

## Global Constraints

- Dependency version is exactly `2.8.17` (confirmed compatible with Spring Boot 3.5.x).
- `OpenAPI` bean info: title `"JobFlowQ API"`, description `"Event-driven job queue with Kafka"`, version `"2.0.0"`.
- No `@ApiResponse`/status-code annotations — out of scope per spec.
- No `@Schema` annotations on `QueueMetrics` — only `JobRequest` and `JobResponse`.
- No existing method bodies, validation, or response shapes change.
- No Lombok.

---

### Task 1: Dependency + OpenApiConfig

**Files:**
- Modify: `pom.xml`
- Create: `src/main/java/com/jobflowq/jobflowq/config/OpenApiConfig.java`

**Interfaces:**
- Produces: an `OpenAPI` bean with the configured title/description/version, picked up automatically by springdoc — no other task depends on this directly, but the Swagger UI's info panel (verified in Task 4) reflects it.

- [ ] **Step 1: Add the dependency**

In `pom.xml`, inside the existing `<dependencies>` block, add (after the `spring-boot-starter-web` dependency, before the `postgresql` dependency):

```xml
		<dependency>
			<groupId>org.springdoc</groupId>
			<artifactId>springdoc-openapi-starter-webmvc-ui</artifactId>
			<version>2.8.17</version>
		</dependency>
```

- [ ] **Step 2: Create OpenApiConfig**

Create `src/main/java/com/jobflowq/jobflowq/config/OpenApiConfig.java`:

```java
package com.jobflowq.jobflowq.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI().info(new Info()
                .title("JobFlowQ API")
                .description("Event-driven job queue with Kafka")
                .version("2.0.0"));
    }
}
```

- [ ] **Step 3: Verify it compiles**

Run: `./mvnw -q compile`
Expected: `BUILD SUCCESS`.

- [ ] **Step 4: Commit**

```bash
git add pom.xml src/main/java/com/jobflowq/jobflowq/config/OpenApiConfig.java
git commit -m "feat: add springdoc-openapi dependency and OpenApiConfig bean"
```

---

### Task 2: Annotate JobController and MetricsController

**Files:**
- Modify: `src/main/java/com/jobflowq/jobflowq/controller/JobController.java`
- Modify: `src/main/java/com/jobflowq/jobflowq/controller/MetricsController.java`

**Interfaces:**
- Consumes: nothing from Task 1 directly (annotations are independent of the config bean).
- Produces: nothing consumed by later tasks — Task 4 verifies these annotations show up in the Swagger UI.

- [ ] **Step 1: Replace JobController.java**

Replace the full contents of `src/main/java/com/jobflowq/jobflowq/controller/JobController.java`:

```java
package com.jobflowq.jobflowq.controller;

import com.jobflowq.jobflowq.dto.JobRequest;
import com.jobflowq.jobflowq.dto.JobResponse;
import com.jobflowq.jobflowq.service.JobService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/jobs")
@Tag(name = "Jobs", description = "Job submission, retrieval, and lifecycle management")
public class JobController {

    private static final Logger logger = LoggerFactory.getLogger(JobController.class);

    private final JobService jobService;

    public JobController(JobService jobService) {
        this.jobService = jobService;
    }

    @Operation(summary = "Submit a new job to the queue")
    @PostMapping
    public ResponseEntity<?> submitJob(@Valid @RequestBody JobRequest request) {
        try {
            JobResponse response = jobService.submitJob(request);
            return ResponseEntity.status(HttpStatus.CREATED).body(response);
        } catch (Exception e) {
            logger.error("Failed to submit job", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to submit job: " + e.getMessage()));
        }
    }

    @Operation(summary = "Retrieve a job by its ID")
    @GetMapping("/{id}")
    public ResponseEntity<?> getJob(@PathVariable Long id) {
        try {
            JobResponse response = jobService.getJob(id);
            return ResponseEntity.ok(response);
        } catch (RuntimeException e) {
            logger.error("Failed to get job id={}", id, e);
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", e.getMessage()));
        }
    }

    @Operation(summary = "List all jobs in the queue")
    @GetMapping
    public ResponseEntity<?> getAllJobs() {
        try {
            List<JobResponse> responses = jobService.getAllJobs();
            return ResponseEntity.ok(responses);
        } catch (Exception e) {
            logger.error("Failed to list jobs", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to list jobs: " + e.getMessage()));
        }
    }

    @Operation(summary = "Cancel a pending job")
    @DeleteMapping("/{id}")
    public ResponseEntity<?> cancelJob(@PathVariable Long id) {
        try {
            JobResponse response = jobService.cancelJob(id);
            return ResponseEntity.ok(response);
        } catch (RuntimeException e) {
            logger.error("Failed to cancel job id={}", id, e);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(Map.of("error", e.getMessage()));
        }
    }
}
```

- [ ] **Step 2: Replace MetricsController.java**

Replace the full contents of `src/main/java/com/jobflowq/jobflowq/controller/MetricsController.java`:

```java
package com.jobflowq.jobflowq.controller;

import com.jobflowq.jobflowq.dto.QueueMetrics;
import com.jobflowq.jobflowq.service.JobService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/metrics")
@Tag(name = "Metrics", description = "Queue-wide job metrics")
public class MetricsController {

    private static final Logger logger = LoggerFactory.getLogger(MetricsController.class);

    private final JobService jobService;

    public MetricsController(JobService jobService) {
        this.jobService = jobService;
    }

    @Operation(summary = "Retrieve aggregate queue metrics")
    @GetMapping
    public ResponseEntity<?> getMetrics() {
        try {
            QueueMetrics metrics = jobService.getMetrics();
            return ResponseEntity.ok(metrics);
        } catch (Exception e) {
            logger.error("Failed to compute metrics", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to compute metrics: " + e.getMessage()));
        }
    }
}
```

- [ ] **Step 3: Verify it compiles**

Run: `./mvnw -q compile`
Expected: `BUILD SUCCESS`.

- [ ] **Step 4: Commit**

```bash
git add src/main/java/com/jobflowq/jobflowq/controller/JobController.java src/main/java/com/jobflowq/jobflowq/controller/MetricsController.java
git commit -m "feat: add OpenAPI @Tag and @Operation annotations to controllers"
```

---

### Task 3: Annotate JobRequest and JobResponse

**Files:**
- Modify: `src/main/java/com/jobflowq/jobflowq/dto/JobRequest.java`
- Modify: `src/main/java/com/jobflowq/jobflowq/dto/JobResponse.java`

**Interfaces:**
- Consumes: nothing from Tasks 1-2.
- Produces: nothing consumed by later tasks — Task 4 verifies these show up in the generated schema.

- [ ] **Step 1: Replace JobRequest.java**

Replace the full contents of `src/main/java/com/jobflowq/jobflowq/dto/JobRequest.java`:

```java
package com.jobflowq.jobflowq.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;

public class JobRequest {

    @Schema(description = "Job type identifier, used by the worker to determine processing behavior (e.g. EMAIL, REPORT, EXPORT)")
    @NotBlank(message = "Job type is required")
    private String type;

    @Schema(description = "Optional company name associated with this job, propagated to published Kafka events")
    private String companyName;

    @Schema(description = "Arbitrary JSON payload for the job, stored as text")
    private String payload = "{}";

    @Schema(description = "Job priority; higher values are processed first")
    private Integer priority = 5;

    @Schema(description = "Maximum number of retry attempts before the job is dead-lettered")
    private Integer maxRetries = 3;

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public String getCompanyName() { return companyName; }
    public void setCompanyName(String companyName) { this.companyName = companyName; }

    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }

    public Integer getPriority() { return priority; }
    public void setPriority(Integer priority) { this.priority = priority; }

    public Integer getMaxRetries() { return maxRetries; }
    public void setMaxRetries(Integer maxRetries) { this.maxRetries = maxRetries; }
}
```

- [ ] **Step 2: Replace JobResponse.java**

Replace the full contents of `src/main/java/com/jobflowq/jobflowq/dto/JobResponse.java`:

```java
package com.jobflowq.jobflowq.dto;

import com.jobflowq.jobflowq.model.JobStatus;
import io.swagger.v3.oas.annotations.media.Schema;
import java.time.LocalDateTime;

public class JobResponse {

    @Schema(description = "Unique identifier of the job")
    private Long id;

    @Schema(description = "Job type identifier (e.g. EMAIL, REPORT, EXPORT)")
    private String type;

    @Schema(description = "Company name associated with this job, if provided")
    private String companyName;

    @Schema(description = "Arbitrary JSON payload for the job")
    private String payload;

    @Schema(description = "Current lifecycle status of the job")
    private JobStatus status;

    @Schema(description = "Job priority; higher values are processed first")
    private Integer priority;

    @Schema(description = "Number of times this job has been retried after failure")
    private Integer retryCount;

    @Schema(description = "Maximum number of retry attempts before the job is dead-lettered")
    private Integer maxRetries;

    @Schema(description = "Identifier of the worker currently or last processing this job")
    private String workerId;

    @Schema(description = "Error message from the most recent failed processing attempt, if any")
    private String errorMessage;

    @Schema(description = "Timestamp when the job was created")
    private LocalDateTime createdAt;

    @Schema(description = "Timestamp when the job was last updated")
    private LocalDateTime updatedAt;

    @Schema(description = "Timestamp when the job completed successfully, if applicable")
    private LocalDateTime completedAt;

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public String getCompanyName() { return companyName; }
    public void setCompanyName(String companyName) { this.companyName = companyName; }

    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }

    public JobStatus getStatus() { return status; }
    public void setStatus(JobStatus status) { this.status = status; }

    public Integer getPriority() { return priority; }
    public void setPriority(Integer priority) { this.priority = priority; }

    public Integer getRetryCount() { return retryCount; }
    public void setRetryCount(Integer retryCount) { this.retryCount = retryCount; }

    public Integer getMaxRetries() { return maxRetries; }
    public void setMaxRetries(Integer maxRetries) { this.maxRetries = maxRetries; }

    public String getWorkerId() { return workerId; }
    public void setWorkerId(String workerId) { this.workerId = workerId; }

    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }

    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }

    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }

    public LocalDateTime getCompletedAt() { return completedAt; }
    public void setCompletedAt(LocalDateTime completedAt) { this.completedAt = completedAt; }
}
```

- [ ] **Step 3: Verify it compiles**

Run: `./mvnw -q compile`
Expected: `BUILD SUCCESS`.

- [ ] **Step 4: Commit**

```bash
git add src/main/java/com/jobflowq/jobflowq/dto/JobRequest.java src/main/java/com/jobflowq/jobflowq/dto/JobResponse.java
git commit -m "feat: add OpenAPI @Schema annotations to JobRequest and JobResponse fields"
```

---

### Task 4: Verification

**Files:** none (verification only)

**Interfaces:**
- Consumes: everything from Tasks 1-3.

- [ ] **Step 1: Run the full automated test suite**

Run: `./mvnw -q test`
Expected: `BUILD SUCCESS`, all existing tests still pass (annotations are metadata-only — no test should change behavior).

- [ ] **Step 2: Start Postgres (required for the app to start)**

Run: `docker compose up -d postgres`
Expected: `jobflowq-db` container `Up`.

- [ ] **Step 3: Start the application**

Run: `./mvnw spring-boot:run` (background)
Expected: startup logs end with `Started JobflowqApplication`, no errors.

- [ ] **Step 4: Verify Swagger UI**

Run: `curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/swagger-ui.html` (follow redirects: add `-L`)
Expected: `200` after following the redirect to `/swagger-ui/index.html`.

- [ ] **Step 5: Verify the generated OpenAPI document**

Run: `curl -s http://localhost:8080/v3/api-docs`
Expected: JSON response containing `"title":"JobFlowQ API"`, `"description":"Event-driven job queue with Kafka"`, `"version":"2.0.0"`, and paths for `/api/jobs`, `/api/jobs/{id}`, `/api/metrics` with their `summary` fields populated from the `@Operation` annotations.

- [ ] **Step 6: Confirm existing endpoints still behave identically**

Run: `curl -s -X POST http://localhost:8080/api/jobs -H "Content-Type: application/json" -d '{"type":"EMAIL","companyName":"Acme Corp"}'`
Expected: `201` with the same JSON shape as before this change (annotations don't alter serialization).

- [ ] **Step 7: Tear down**

Stop the application, then `docker compose down`.

- [ ] **Step 8: Report results**

Summarize pass/fail for each step. If any step fails, stop and report rather than marking the plan complete.
