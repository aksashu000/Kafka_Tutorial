package com.ashu.tutorial.consumers.generic;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerTest {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "CountryCounter");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("quickstart-events"));
        /*
         Example: To subscribe to all test topics, we can call:
         consumer.subscribe(Pattern.compile("test.*"));
         */

        /*
        This is indeed an infinite loop. Consumers are usually long-running applications that continuously poll Kafka for more data.
        The parameter we pass to poll() is a timeout interval and controls how long poll() will block if data is not available
        in the consumer buffer. If this is set to 0 or if there are records available already, poll() will return immediately;
        otherwise, it will wait for the specified number of milliseconds.
         */

        Duration timeout = Duration.ofMillis(1000);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(timeout);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("topic = %s, partition = %d, offset = %d, key = %s, value = %s\n",
                        record.topic(), record.partition(), record.offset(),
                        record.key(), record.value());

            }
        }
    }
}
