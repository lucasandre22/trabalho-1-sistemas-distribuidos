package com.all.producer;

import com.all.PropertiesHelper;
import com.all.Record;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer
{
    private final KafkaProducer<String, String> kafkaProducer;
    private final String topicName;

    public Producer(String topicName) {
        this.kafkaProducer = new KafkaProducer<>(PropertiesHelper.initializeAndGetProperties());
        this.topicName = topicName;
    }

    public void sendRecord(Record record, Callback callback) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, record.getRecordName());
        System.out.println("Producer sending record " + record);
        kafkaProducer.send(producerRecord, callback);
        kafkaProducer.flush();
    }


}
