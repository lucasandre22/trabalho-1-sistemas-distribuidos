package com.all;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import java.util.Properties;

public class PropertiesHelper {
    private static Properties properties;

    static {
        initializeProperties("localhost:9092", "test-group");
    }

    public static void initializeProperties(String bootstrapServer, String groupId) {
        properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put("buffer.memory", 33554432);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", groupId);
    }

    public static Properties getProperties() {
        if (properties == null) {
            throw new IllegalStateException("Properties have not been initialized. Call initializeProperties() first.");
        }
        return properties;
    }
}
