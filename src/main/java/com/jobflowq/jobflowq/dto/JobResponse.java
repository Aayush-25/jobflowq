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
