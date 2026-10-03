package com.jobflowq.jobflowq.kafka;

import com.jobflowq.jobflowq.dto.JobApplicationEvent;
import com.jobflowq.jobflowq.model.JobStatus;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class JobEventConsumerTest {

    @Test
    void onEventLogsWithoutThrowing() {
        JobEventConsumer consumer = new JobEventConsumer();
        JobApplicationEvent event = new JobApplicationEvent(1L, "Acme Corp", JobStatus.PENDING, LocalDateTime.now(), "CREATED");

        assertDoesNotThrow(() -> consumer.onEvent(event));
    }
}
