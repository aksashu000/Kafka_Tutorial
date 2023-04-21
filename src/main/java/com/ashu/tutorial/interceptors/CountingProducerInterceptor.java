package com.ashu.tutorial.interceptors;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


/*
Print number of messages sent and messages acknowledged every N milliseconds
 */
public class CountingProducerInterceptor implements ProducerInterceptor {

    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    static AtomicLong totalSentMessages = new AtomicLong(0);
    static AtomicLong totalAcknowledgedMessages = new AtomicLong(0);

    public ProducerRecord onSend(ProducerRecord producerRecord) {
        totalSentMessages.incrementAndGet();
        return producerRecord;
    }

    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        totalSentMessages.incrementAndGet();
    }

    public void close() {
        executorService.shutdownNow();
    }

    public void configure(Map<String, ?> map) {
        long windowSize = Long.parseLong((String) map.get("counting.interceptor.window.size.ms"));
        executorService.scheduleAtFixedRate(CountingProducerInterceptor::run, windowSize, windowSize, TimeUnit.MILLISECONDS);
    }

    public static void run() {
        System.out.println("Total sent: " + totalSentMessages.getAndSet(0));
        System.out.println("Total acknowledged: " + totalAcknowledgedMessages.getAndSet(0));
    }
}
