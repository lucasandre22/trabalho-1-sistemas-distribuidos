package com.all;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class Main {
    public static void main(String args[]) {
        Producer producerTanque = new Producer("sensor_tanque");
        Producer producerBomba = new Producer("sensor_bomba");

        Consumer consumerAdministrativo = new Consumer("sensor_tanque", "consumer-1");
        Consumer consumerDashboard = new Consumer("sensor_bomba", "consumer-1");
        Consumer consumerCaixa = new Consumer("sensor_tanque", "consumer-2");


        Callback callback = createProducerCallback();

        setupConsumer(consumerDashboard,  "consumer_dashboard");
        setupConsumer(consumerCaixa, "consumer_caixa");
        setupConsumer(consumerAdministrativo,"consumer_administrativo");

        producerTanque.sendRecord("500L", callback);
        producerBomba.sendRecord("30L, 150", callback);
        producerBomba.sendRecord("20L, 120", callback);
        producerTanque.sendRecord("1500L", callback);
    }

    private static Callback createProducerCallback() {
        return new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    System.out.println("\n" + "Received new metadata. \n" +
                            "Topic:" + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp() + "\n");
                } else {
                    System.err.println("Error while producing");
                    e.printStackTrace();
                }
            }
        };
    }

    private static void setupConsumer(Consumer consumer, String consumerName) {
        consumer.startListening(consumerName, createConsumerCallback(consumerName));
    }

    private static ConsumerMessageHandlerCallback createConsumerCallback(String consumerName) {
        return new ConsumerMessageHandlerCallback() {
            @Override
            public void processRecord(String topicName, ConsumerRecord<?, ?> record) {
                System.out.println("The consumer: " + consumerName + " received a new " + topicName + " record from producer.");
                System.out.println(record);
            }
        };
    }
}



