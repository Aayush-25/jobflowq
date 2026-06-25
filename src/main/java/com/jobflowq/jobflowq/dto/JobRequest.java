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
