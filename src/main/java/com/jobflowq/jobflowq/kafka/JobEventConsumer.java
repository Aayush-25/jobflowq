package com.jobflowq.jobflowq.kafka;

import com.jobflowq.jobflowq.config.KafkaConfig;
import com.jobflowq.jobflowq.dto.JobApplicationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class JobEventConsumer {

    private static final Logger logger = LoggerFactory.getLogger(JobEventConsumer.class);

    @KafkaListener(topics = JobEventProducer.TOPIC, groupId = KafkaConfig.CONSUMER_GROUP_ID)
    public void onEvent(JobApplicationEvent event) {
        logger.info("Received job event: {}", event);
    }
}
