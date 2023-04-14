package com.ashu.tutorial.producers.generic;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/*
Sending a message synchronously is simple but still allows the producer to catch
exceptions when Kafka responds to the produce request with an error, or when send
retries were exhausted.

If you send messages synchronously, the sending thread will spend this time waiting
and doing nothing else, not even sending additional messages. This leads to very poor
performance, and as a result, synchronous sends are usually not used in production applications.
 */
public class ProducerTestSynchronous {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try{
            Producer<String, String> producer = new KafkaProducer<>(kafkaProps);
            ProducerRecord<String, String> record = new ProducerRecord<>("quickstart-events", "someKey", "Hello Kakfa!!!");

            //Use get() to wait on the Future and see if send() was successful or not before sending the next record.
            producer.send(record).get();
            producer.close();
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
