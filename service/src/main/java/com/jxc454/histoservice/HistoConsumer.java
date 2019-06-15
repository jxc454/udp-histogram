package com.jxc454.histoservice;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import com.jxc454.models.SimpleMessages.SimpleInt;


public class HistoConsumer {
    private final static String TOPIC = "histogram";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    private static Consumer<SimpleInt, SimpleInt> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "histo-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, PbIntDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, PbIntDeserializer.class);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);

        // Create the consumer using props.
        final Consumer<SimpleInt, SimpleInt> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }

    static void runConsumer() throws InterruptedException {
        final Consumer<SimpleInt, SimpleInt> consumer = createConsumer();

        int trips = 0;
        int noRecordsCount = 0;
        int giveUp = 5;

        while (trips == 0) {
            final ConsumerRecords<SimpleInt, SimpleInt> consumerRecords = consumer.poll(Duration.ofMillis(350));

            if (consumerRecords.count() == 0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            trips++;

            consumerRecords.forEach(record -> {
                SimpleInt simpleIntKey = record.key();
                SimpleInt simpleIntValue = record.value();

                System.out.printf("Consumer Record:(%d %d)\n", simpleIntKey.getIntValue(), simpleIntValue.getIntValue());
            });
            consumer.commitAsync();
        }

        consumer.close();
        System.out.println("DONE");
    }
}
