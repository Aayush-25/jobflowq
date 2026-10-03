package com.jobflowq.jobflowq.service;

import com.jobflowq.jobflowq.dto.JobApplicationEvent;
import com.jobflowq.jobflowq.dto.JobRequest;
import com.jobflowq.jobflowq.dto.JobResponse;
import com.jobflowq.jobflowq.kafka.JobEventProducer;
import com.jobflowq.jobflowq.model.Job;
import com.jobflowq.jobflowq.model.JobStatus;
import com.jobflowq.jobflowq.repository.JobRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class JobServiceTest {

    @Mock
    private JobRepository jobRepository;

    @Mock
    private JobEventProducer jobEventProducer;

    private JobService newService() {
        return new JobService(jobRepository, jobEventProducer);
    }

    @Test
    void submitJobSavesCompanyNameAndPublishesCreatedEvent() {
        JobService jobService = newService();
        when(jobRepository.save(any(Job.class))).thenAnswer(invocation -> {
            Job job = invocation.getArgument(0);
            job.setId(100L);
            return job;
        });

        JobRequest request = new JobRequest();
        request.setType("EMAIL");
        request.setCompanyName("Acme Corp");

        JobResponse response = jobService.submitJob(request);

        assertEquals("Acme Corp", response.getCompanyName());
        assertEquals(100L, response.getId());

        ArgumentCaptor<JobApplicationEvent> captor = ArgumentCaptor.forClass(JobApplicationEvent.class);
        verify(jobEventProducer).publish(captor.capture());
        JobApplicationEvent event = captor.getValue();
        assertEquals(100L, event.applicationId());
        assertEquals("Acme Corp", event.companyName());
        assertEquals(JobStatus.PENDING, event.status());
        assertEquals("CREATED", event.eventType());
    }

    @Test
    void cancelJobWhenPendingPublishesStatusUpdatedEvent() {
        JobService jobService = newService();
        Job existing = new Job();
        existing.setId(5L);
        existing.setType("REPORT");
        existing.setCompanyName("Globex");
        existing.setStatus(JobStatus.PENDING);
        when(jobRepository.findById(5L)).thenReturn(Optional.of(existing));
        when(jobRepository.save(any(Job.class))).thenAnswer(invocation -> invocation.getArgument(0));

        JobResponse response = jobService.cancelJob(5L);

        assertEquals(JobStatus.FAILED, response.getStatus());

        ArgumentCaptor<JobApplicationEvent> captor = ArgumentCaptor.forClass(JobApplicationEvent.class);
        verify(jobEventProducer).publish(captor.capture());
        JobApplicationEvent event = captor.getValue();
        assertEquals(5L, event.applicationId());
        assertEquals("Globex", event.companyName());
        assertEquals(JobStatus.FAILED, event.status());
        assertEquals("STATUS_UPDATED", event.eventType());
    }

    @Test
    void cancelJobWhenNotPendingDoesNotPublishEvent() {
        JobService jobService = newService();
        Job existing = new Job();
        existing.setId(6L);
        existing.setStatus(JobStatus.PROCESSING);
        when(jobRepository.findById(6L)).thenReturn(Optional.of(existing));

        assertThrows(RuntimeException.class, () -> jobService.cancelJob(6L));

        verify(jobEventProducer, never()).publish(any());
    }
}
