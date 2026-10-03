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
