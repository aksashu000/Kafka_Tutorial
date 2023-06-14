package com.ashu.tutorial.kafkastreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

public class WordCount {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source = builder.stream("wordcount-test");

        final Pattern pattern = Pattern.compile("\\W+");

        KTable<Object, String> counts =
        source
        .flatMapValues(value-> Arrays.asList(pattern.split(value.toLowerCase())))
        .map((key, value) -> new KeyValue<Object, Object>(value, value))
        .filter((key, value) -> (!value.equals("the")))
        .groupByKey()
        .count()
        .mapValues(value->Long.toString(value));

        counts
        .toStream()
        .foreach((word, count) -> System.out.println("word: " + word + " -> " + count));


        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp(); // Do not use in prod
        streams.start();
    }
}
