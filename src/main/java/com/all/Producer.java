package com.all;

import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Getter
@Setter
public class Producer
{
    private KafkaProducer<String, String> kafkaProducer;
    private String topicName;
    private Callback callback;

    public Producer(String topicName) {
        this.kafkaProducer = new KafkaProducer<>(PropertiesHelper.getProperties());
        this.topicName = topicName;
    }

    public void sendRecord(Record record) {
        this.sendRecord(record, this.callback);
    }

    public void sendRecord(Record record, Callback callback) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topicName, record.getAd());
        System.out.println("Sending new record at topic " + topicName);
        kafkaProducer.send(producerRecord, callback);
        kafkaProducer.flush();
        //kafkaProducer.close();
    }


}
