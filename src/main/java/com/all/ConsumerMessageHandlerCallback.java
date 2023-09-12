package com.all;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerMessageHandlerCallback
{
    void processRecord(String topicName, ConsumerRecord<?, ?> record);
}
