# SpringDoc OpenAPI Integration for JobFlowQ

## Context

JobFlowQ's REST API (`JobController` at `/api/jobs`, `MetricsController` at
`/api/metrics`) currently has no machine-readable API documentation. This
spec adds SpringDoc OpenAPI annotations and a Swagger UI so the API is
self-documenting, with zero changes to existing request/response behavior.

## Goals

- Add `springdoc-openapi-starter-webmvc-ui` (version 2.8.17, confirmed
  compatible with Spring Boot 3.5.x) to `pom.xml`.
- Add `@Tag` to `JobController` and `MetricsController`.
- Add `@Operation(summary = "...")` to every endpoint in both controllers
  (5 endpoints total: submit, get-by-id, list, cancel, metrics).
- Add `@Schema(description = "...")` to every field of `JobRequest` and
  `JobResponse`.
- Expose Swagger UI at `/swagger-ui.html`.
- Add `OpenApiConfig.java` with a top-level `OpenAPI` bean: title
  "JobFlowQ API", description "Event-driven job queue with Kafka", version
  "2.0.0".
- No existing method bodies, validation, or response shapes change —
  annotations and one new config class only.

## Non-goals

- No `@ApiResponse`/status-code documentation — not requested, and adding
  it would expand scope beyond "every endpoint with a clear summary."
- No `@Schema` annotations on `QueueMetrics` — only `JobRequest` and
  `JobResponse` were named in the request.
- No authentication/security scheme configuration for Swagger UI (matches
  the project's existing no-auth-on-local-services pattern).

## Design

### 1. Dependency

`pom.xml`: add, in the same dependencies block as the other starters:
```xml
<dependency>
    <groupId>org.springdoc</groupId>
    <artifactId>springdoc-openapi-starter-webmvc-ui</artifactId>
    <version>2.8.17</version>
</dependency>
```

### 2. OpenApiConfig

New file `src/main/java/com/jobflowq/jobflowq/config/OpenApiConfig.java`
(same package as the existing `KafkaConfig`):
```java
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

### 3. Controller annotations

`JobController`: `@Tag(name = "Jobs", description = "Job submission, retrieval, and lifecycle management")` on the class. `@Operation(summary = "...")` on each of: `submitJob`, `getJob`, `getAllJobs`, `cancelJob`.

`MetricsController`: `@Tag(name = "Metrics", description = "Queue-wide job metrics")` on the class. `@Operation(summary = "...")` on `getMetrics`.

### 4. DTO annotations

`@Schema(description = "...")` added to every field declaration in
`JobRequest` (type, companyName, payload, priority, maxRetries) and
`JobResponse` (id, type, companyName, payload, status, priority,
retryCount, maxRetries, workerId, errorMessage, createdAt, updatedAt,
completedAt). Descriptions are derived from existing field semantics
already established elsewhere in the codebase (e.g. `schema.sql` comments,
`JobWorkerService` retry logic) — no new behavior implied by the
descriptions.

### 5. Swagger UI

No additional config needed — `springdoc-openapi-starter-webmvc-ui`
registers `/swagger-ui.html` as a redirect to `/swagger-ui/index.html` by
default. Verified manually during implementation (start the app, hit the
URL, confirm it renders) rather than assumed.

## Testing

- `./mvnw -q compile` to confirm annotations don't break compilation.
- `./mvnw -q test` to confirm the full existing suite still passes
  (annotations are metadata-only, no logic touched).
- Manual: start the app, hit `GET /swagger-ui.html`, confirm it loads and
  renders all 5 endpoints with their summaries, confirm `GET
  /v3/api-docs` returns the OpenAPI JSON with the configured
  title/description/version.
