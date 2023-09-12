package com.all;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerMessageHandlerCallback
{
    public void processRecord(String topicName, ConsumerRecord<String, String> record);
}
