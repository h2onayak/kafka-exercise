package consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }
    private ConsumerDemoWithThread(){}
    private void run(){
        final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "consumer_group_2";
        String topic ="first_topic";

        //latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);
        //Create the consumer runnable
        logger.info("Creating the consumer thread.");
        Runnable myConsumerRunnable = new ConsumerRunnable(bootstrapServer,
                groupId,
                topic,
                latch);
        //Start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Caught shutdown hook.");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.error("Application got interrupted ",e);
            }finally {
                logger.info("Application is closing.");
            }
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted ",e);
        }finally {
            logger.info("Application is closing.");
        }
    }
    public class ConsumerRunnable implements Runnable{
        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        public ConsumerRunnable(String bootstrapServers,
                                String groupId,
                                String topic,
                                CountDownLatch latch
                                ){
            this.latch = latch;
            //Crete consumer configs
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

            //Create consumer
            consumer = new KafkaConsumer<String, String>(properties);

            //Subscribe consumer to topic
            consumer.subscribe(Collections.singleton(topic));

        }

        @Override
        public void run() {
            try {
                //Poll for new data
                while (true){
                    ConsumerRecords<String,String> records =
                            consumer.poll(Duration.ofMillis(100));
                    records.forEach(record->{
                        logger.info("key: {}, value: {}", record.key(),record.value());
                        logger.info("Partition: {}, Offset: {}",record.partition(),record.offset());
                    });
                }
            }catch (WakeupException e){
                logger.info("Received shutdown signal");
            }finally {
                consumer.close();
                //tell our main code, we're done with consumer
                latch.countDown();
            }
        }
        public void shutdown(){
            //the wakeup() method is special method to interrupt consumer.poll()
            //it will throw the exception WakeupException
            consumer.wakeup();
        }
    }
}
