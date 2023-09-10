package com.all;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer
{
    private KafkaProducer<String, String> kafkaProducer;
    private String topicName;

    public Producer(String topicName) {
        this.kafkaProducer = new KafkaProducer<>(PropertiesHelper.getProperties());
        this.topicName = topicName;
    }

    public void sendRecord(String recordValue, Callback callback) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, recordValue);
        System.out.println("Sending record..");
        kafkaProducer.send(producerRecord, callback);
        kafkaProducer.flush();
        kafkaProducer.close();
    }


}
