package com.ashu.tutorial.producers.generic;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/*
To send messages asynchronously and still handle error scenarios, the producer supports
adding a callback when sending a record

We can call the 'send()' method with a callback function, which gets triggered when it
receives a response from the Kafka broker.
 */

class ProducerCallbackTest implements Callback {
    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        System.out.println("e = " + e);
        if (e != null) {
            e.printStackTrace();
        }
    }
}

public class ProducerTestAsynchronous {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try{
            Producer<String, String> producer = new KafkaProducer<>(kafkaProps);
            ProducerRecord<String, String> record = new ProducerRecord<>("quickstart-events", "someKey", "Hello Kakfa!!!");
            producer.send(record, new ProducerCallbackTest());
            producer.close();
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
