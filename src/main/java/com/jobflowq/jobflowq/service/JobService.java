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
