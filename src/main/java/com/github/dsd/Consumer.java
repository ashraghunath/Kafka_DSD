package com.github.dsd;

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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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

public class Consumer {

    public static void main(String[] args) throws IOException {

        long startTime=0L;
try {
    Logger logger = LoggerFactory.getLogger(Consumer.class.getName());


    RestHighLevelClient client = createClient();


    String jsonString = "";


    startTime = System.nanoTime();
    KafkaConsumer<String, String> consumer = createConsumer("example_topic3");
    while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, String> record : records) {


                logger.info("Key : "+record.key()+" Value : "+ record.value());
                logger.info("Partition : "+record.partition()+" Offset : "+record.offset());


//            IndexRequest indexRequest = new IndexRequest("kafka")
//                    .source(record.value(), XContentType.JSON);
//            IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
//            String id = indexResponse.getId();
//            logger.info(id);
        }
    }
}catch (Exception e)
{
    long end = System.nanoTime();
    System.out.println(end - startTime);
}
//        client.close();
    }

    public static KafkaConsumer<String,String> createConsumer(String topic){
        String bootstrapServer = "18.118.168.128:9092";
        String groupId = "my-first-application";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList(topic));
        return kafkaConsumer;

    }

    public static RestHighLevelClient createClient()
    {
        String hostname="kafka-example-3164371506.us-east-1.bonsaisearch.net";
        String username="dz80tuqdj0";
        String password="oa3qbv3pkb";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(username,password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname,443,"https"))
                .setHttpClientConfigCallback(new RestClientBuilder
                        .HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }
}
