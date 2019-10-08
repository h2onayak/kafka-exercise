package com.hanumanta.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // Create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        for(int i=0; i<=10; i++) {
            // Create producer record
            final ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world"+Integer.toString(i));
            // Send data - asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received new metadata. \n " +
                                    "Topic: {}\n " +
                                    "Partition: {} \n " +
                                    "Offset: {}\n" +
                                    " Timestamp: {}", recordMetadata.topic(),
                                recordMetadata.partition(),
                                recordMetadata.offset(),
                                recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing ", e);
                    }
                }
            });
        }
        //flush data
        producer.flush();
        //flush and close data
        producer.close();

    }
}
