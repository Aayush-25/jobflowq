package com.jobflowq.jobflowq.kafka;

import com.jobflowq.jobflowq.dto.JobApplicationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class JobEventProducer {

    public static final String TOPIC = "job-application-events";

    private static final Logger logger = LoggerFactory.getLogger(JobEventProducer.class);

    private final KafkaTemplate<String, JobApplicationEvent> kafkaTemplate;

    public JobEventProducer(KafkaTemplate<String, JobApplicationEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(JobApplicationEvent event) {
        try {
            kafkaTemplate.send(TOPIC, String.valueOf(event.applicationId()), event)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            logger.error("Failed to publish job event for applicationId={}", event.applicationId(), ex);
                        }
                    });
        } catch (Exception e) {
            logger.error("Failed to publish job event for applicationId={}", event.applicationId(), e);
        }
    }
}
