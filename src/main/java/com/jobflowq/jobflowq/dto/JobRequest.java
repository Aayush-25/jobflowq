package com.jobflowq.jobflowq.dto;

import jakarta.validation.constraints.NotBlank;

public class JobRequest {

    @NotBlank(message = "Job type is required")
    private String type;

    private String companyName;

    private String payload = "{}";
    private Integer priority = 5;
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
