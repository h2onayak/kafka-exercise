package elasticsearch.consumer;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    private static RestHighLevelClient createClient(){
        String hostName="";
        String username="";
        String password="";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username,password));
        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostName,443,"https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    private static KafkaConsumer<String, String> createConsumer(String topic) {

        String bootStrapServer = "127.0.0.1:9092";
        String groupId = "consumer_demo_elasticsearch";
        //Crete consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");//disable auto commit of offsets.
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"100");
        //Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
       consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    private static String extractIdFromTwitter(String tweetJson) {
        return JsonParser.parseString(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }
    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client =createClient();
        KafkaConsumer<String,String> consumer = createConsumer("twitter_topic");
        while (true){
            ConsumerRecords<String,String> records =
                    consumer.poll(Duration.ofMillis(100));
            int recordsCount = records.count();
            logger.info("Received {} records",recordsCount);
           // BulkRequest bulkRequest = new BulkRequest(); // TO handle requests by batching.
            for (ConsumerRecord<String, String> record : records) {
                //2 strategies to create unique id
                //1. Kafka generic id
               //String id = record.topic()+"_"+record.partition()+"_"+record.offset();
                //2. twitter feed specific id
                try {
                    String id = extractIdFromTwitter(record.value());
                    logger.info("ID : {}",id);
                    //Insert data into ElasticSearch
                    IndexRequest indexRequest = new IndexRequest("twitter")
                            .id(id) // this is to make sure consumer idempotent.
                            .source(record.value(), XContentType.JSON);
                    IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                    logger.info("IndexResponse id : {} ", indexResponse.getId());
                    //    bulkRequest.add(indexRequest);
                }catch (NullPointerException e){
                    logger.warn("Skipping bad data: {} ",record.value());
                }

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
//            if(recordsCount > 0){
//                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
//            }
            logger.info("Committing offsets...");
            consumer.commitSync();
            logger.info("Offsets have been committed.");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        //Close client
      //  client.close();
    }
}
