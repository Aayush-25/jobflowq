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
