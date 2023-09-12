package com.all;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class Consumer
{
    private KafkaConsumer<String, String> kafkaConsumer;
    private final String topicName;
    private final long TIME_OUT_MS = 500;

    public Consumer(String topicName, String consumerGroup) {
        PropertiesHelper.initializeProperties("localhost:9092", consumerGroup);
        this.kafkaConsumer = new KafkaConsumer<>(PropertiesHelper.getProperties());
        this.topicName = topicName;
    }

    public void startListening(String consumerName, ConsumerMessageHandlerCallback callback) {
        kafkaConsumer.subscribe(List.of(topicName));

        Thread thread = new Thread()
        {
            @Override public void run()
            {
                System.out.println("The consumer " + consumerName + " started listening for topics at " + topicName);
                while(true) {
                    ConsumerRecords<String, String> records =
                            kafkaConsumer.poll(Duration.ofMillis(TIME_OUT_MS));
                    for (ConsumerRecord<String, String> record : records) {
                        callback.processRecord(topicName, record);
                    }
                }
            }
        };
        thread.start();
    }

}
