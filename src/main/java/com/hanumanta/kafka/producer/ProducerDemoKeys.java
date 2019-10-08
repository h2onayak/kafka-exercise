package com.hanumanta.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // Create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        for(int i=0; i<=10; i++) {
            // Create producer record
            String topic = "first_topic";
            String value = "hello world"+Integer.toString(i);
            String key ="id_"+Integer.toString(i);

            logger.info("key: {}",key);
            final ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic,key,value );
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
            }).get(); // block the .send() to make it synchronous -- don't do this in prod.
            // run at least two times to check same key will goes to same partition.
        }
        //flush data
        producer.flush();
        //flush and close data
        producer.close();

    }
}
