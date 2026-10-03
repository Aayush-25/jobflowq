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
