package com.all;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class Main
{
    public static void main(String[] args) {
        Producer anuncioCasas = createProducer("anuncio_casas");
        Producer anuncioCarros = createProducer("anuncio_carros");
        Producer anuncioBarcos = createProducer("anuncio_barcos");
        Consumer consumerCasas = createConsumer("anuncio_casas");
        Consumer consumerCarros = createConsumer("anuncio_carros");
        Consumer consumerBarcos = createConsumer("anuncio_barcos");

        anuncioCasas.sendRecord(Record.HOUSE_AD);
        anuncioCasas.sendRecord(Record.APPARTMENT_AD);
        anuncioCarros.sendRecord(Record.TIGUAN_AD);
        anuncioCarros.sendRecord(Record.VOYAGE_AD);
        anuncioBarcos.sendRecord(Record.YATCH_AD);
        anuncioBarcos.sendRecord(Record.BOAT_AD);

    }

    public static Consumer createConsumer(String topicName) {
        Consumer consumer = new Consumer(topicName);
        consumer.startListening(new ConsumerMessageHandlerCallback()
        {
            @Override
            public void processRecord(String topicName, ConsumerRecord<String, String> record)
            {
                System.out.println("The consumer from topic " + topicName + " received a new record: ");
                System.out.println(record.value());
            }
        });
        return consumer;
    }

    public static Producer createProducer(String topicName) {
        Producer producer = new Producer(topicName);
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
        producer.setCallback(callback);
        return producer;
        //producer.sendRecord("casa 50 metros quadrados", callback);
    }
}
