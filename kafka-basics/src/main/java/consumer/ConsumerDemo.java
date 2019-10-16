package consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

        String bootStrapServer = "127.0.0.1:9092";
        String groupId = "consumer_group_1";
        String topic ="first_topic";
        //Crete consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //Create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        //Subscribe consumer to topic
        consumer.subscribe(Collections.singleton(topic));
        //Poll for new data
        while (true){
           ConsumerRecords<String,String> records =
                   consumer.poll(Duration.ofMillis(100));
           records.forEach(record->{
               logger.info("key: {}, value: {}", record.key(),record.value());
               logger.info("Partition: {}, Offset: {}",record.partition(),record.offset());
           });
        }
    }
}
