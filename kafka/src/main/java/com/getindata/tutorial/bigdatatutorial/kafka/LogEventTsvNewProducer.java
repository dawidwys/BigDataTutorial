package com.getindata.tutorial.bigdatatutorial.kafka;

import com.getindata.tutorial.bigdatatutorial.utils.LogEventTsvGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author wysakowd
 * @since 2015-10-06
 */
public class LogEventTsvNewProducer {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        if (args.length < 2) {
            System.err.println("Usage: LogEventTsvProducer <kafkaBroker1:port,kafkaBroker2:port> <topic>");
            System.exit(1);
        }

        final Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        final String topic = args[2];

        final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        final LogEventTsvGenerator generator = new LogEventTsvGenerator();

        for (int i = 0; i < 10000; i++) {

            final String logEvent = generator.next();

            final ProducerRecord<String, String> data =
                    new ProducerRecord<String, String>(topic, generator.getKey(), logEvent);

            producer.send(data).get();

            if (i % 200 == 0) {
                Thread.sleep(500);
                System.out.println("Produced events: " + i);
            }
        }

        producer.close();
    }
}
