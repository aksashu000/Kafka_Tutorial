package com.ashu.tutorial.producers.avro;

import com.ashu.tutorial.utils.CustomerGenerator;
import customerManagement.avro.Customer;
import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class AvroProducerTest {
    public static void main(String[] args) {
        Properties props = new Properties();
        String schemaUrl = "http://localhost:8081";
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", schemaUrl);

        String topic = "customerContacts";
        Producer<String, Customer> producer = new KafkaProducer<>(props);
        Schema.Parser parser = new Schema.Parser();
        for(int i=1; i<=2; i++) {
            Customer customer = CustomerGenerator.getNext();
            System.out.println("Generated customer " + customer);
            ProducerRecord<String, Customer> record = new ProducerRecord<>(topic, (String) customer.getName(), customer);
            producer.send(record);
        }
        producer.close();
    }
}
