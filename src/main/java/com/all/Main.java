package com.all;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class Main
{
    public static void main(String args[]) {
        Producer producer = new Producer("anuncio_casas");
        Consumer consumer = new Consumer("anuncio_casas");
        Callback callback = new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    System.out.println("Received new metadata. \n" +
                            "Topic:" + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    System.err.println("Error while producing");
                    e.printStackTrace();
                }
            }
        };
        consumer.startListening(new ConsumerMessageHandlerCallback()
        {
            @Override
            public void processRecord(String topicName, ConsumerRecord<?, ?> record)
            {
                System.out.println("The consumer received a new " + topicName + " record from producer.");
                System.out.println(record);
            }
        });
        producer.sendRecord("casa 50 metros quadrados", callback);
    }
}
