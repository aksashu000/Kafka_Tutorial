package com.ashu.tutorial.producers.generic;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/*
We send a message to the server and do not really care if it arrives successfully or
not. In case of errors that can't be retried or timeout, messages will get lost and the application will not
get any information or exceptions about this.
 */
public class ProducerTestFireAndForget {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try{
            Producer<String, String> producer = new KafkaProducer<>(kafkaProps);
            ProducerRecord<String, String> record = new ProducerRecord<>("quickstart-events", "someKey", "Hello Kakfa!!!");
            producer.send(record);
            producer.close();
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
