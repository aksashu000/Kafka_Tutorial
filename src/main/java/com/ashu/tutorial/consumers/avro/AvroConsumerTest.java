package com.ashu.tutorial.consumers.avro;

import customerManagement.avro.Customer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


public class AvroConsumerTest {
    public static void main(String[] args) {
        Duration timeout = Duration.ofMillis(100);
        String schemaUrl = "http://localhost:8081";
        Properties props = new Properties();

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "CustomerGroup");
        props.put("key.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        props.put("schema.registry.url", schemaUrl);

        String topic = "customerContacts";

        KafkaConsumer<String, Customer> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        System.out.println("Reading topic:" + topic);

        //The below while loop will keep running unless you stop the program manually
        while (true) {
            ConsumerRecords<String, Customer> records = consumer.poll(timeout);
            for (ConsumerRecord<String, Customer> record: records) {
                System.out.println("Customer record: " + record.value());
            }
            consumer.commitSync();
        }
    }
}
