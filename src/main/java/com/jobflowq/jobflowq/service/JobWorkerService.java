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
