package com.all;

import com.all.consumer.Consumer;
import com.all.producer.Producer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        Producer producerTanque = new Producer("sensor_tanque");
        Producer producerBomba = new Producer("sensor_bomba");

        Consumer consumerAdministrativo = new Consumer("sensor_tanque", "consumer-1");
        Consumer consumerDashboard = new Consumer("sensor_bomba", "consumer-2");
        Consumer consumerCaixa = new Consumer("sensor_tanque", "consumer-3");


        Callback callback = createProducerCallback();

        setupConsumer(consumerDashboard,  "consumer_dashboard");
        setupConsumer(consumerCaixa, "consumer_caixa");
        setupConsumer(consumerAdministrativo,"consumer_administrativo");

        Thread.sleep(1000);

        producerTanque.sendRecord(Record.TANQUE_1500L, callback);
        Thread.sleep(500);
        producerBomba.sendRecord(Record.BOMBA_30L, callback);
        Thread.sleep(500);
        producerBomba.sendRecord(Record.BOMBA_20L, callback);
        Thread.sleep(500);
        producerTanque.sendRecord(Record.TANQUE_1500L, callback);
    }

    private static Callback createProducerCallback() {
        return new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    System.out.println("\n" + "Received new record metadata. \n" +
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
                System.out.println("The consumer \"" + consumerName + "\" received a new " + topicName + " record from producer with value: " + record.value());
            }
        };
    }
}



