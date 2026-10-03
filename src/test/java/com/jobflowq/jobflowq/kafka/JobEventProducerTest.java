package com.jobflowq.jobflowq.kafka;

import com.jobflowq.jobflowq.dto.JobApplicationEvent;
import com.jobflowq.jobflowq.model.JobStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class JobEventProducerTest {

    @Mock
    private KafkaTemplate<String, JobApplicationEvent> kafkaTemplate;

    @Test
    void publishSendsEventToTopicWithJobIdAsKey() {
        JobEventProducer producer = new JobEventProducer(kafkaTemplate);
        JobApplicationEvent event = new JobApplicationEvent(42L, "Acme Corp", JobStatus.PENDING, LocalDateTime.now(), "CREATED");
        when(kafkaTemplate.send(eq(JobEventProducer.TOPIC), eq("42"), eq(event)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        producer.publish(event);

        verify(kafkaTemplate).send(JobEventProducer.TOPIC, "42", event);
    }

    @Test
    void publishDoesNotThrowWhenSendFailsAsynchronously() {
        JobEventProducer producer = new JobEventProducer(kafkaTemplate);
        JobApplicationEvent event = new JobApplicationEvent(7L, "Globex", JobStatus.FAILED, LocalDateTime.now(), "STATUS_UPDATED");
        CompletableFuture<SendResult<String, JobApplicationEvent>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("broker down"));
        when(kafkaTemplate.send(eq(JobEventProducer.TOPIC), eq("7"), eq(event))).thenReturn(failed);

        producer.publish(event);

        verify(kafkaTemplate).send(JobEventProducer.TOPIC, "7", event);
    }

    @Test
    void publishDoesNotThrowWhenSendThrowsSynchronously() {
        JobEventProducer producer = new JobEventProducer(kafkaTemplate);
        JobApplicationEvent event = new JobApplicationEvent(9L, "Initech", JobStatus.DEAD, LocalDateTime.now(), "STATUS_UPDATED");
        when(kafkaTemplate.send(eq(JobEventProducer.TOPIC), eq("9"), eq(event)))
                .thenThrow(new RuntimeException("serialization error"));

        producer.publish(event);
    }
}
