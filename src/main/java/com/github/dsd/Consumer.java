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
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

public class Consumer {
    static Logger logger = LoggerFactory.getLogger(Consumer.class.getName());
    public static void main(String[] args) throws IOException {

        long startTime=0L;
try {

    RestHighLevelClient client = createClient();

    String jsonString = "";

    startTime = System.nanoTime();
    KafkaConsumer<String, String> consumer = createConsumer("example_topic5");

    GetIndexRequest getIndexRequest = new GetIndexRequest();
    getIndexRequest.indices("kafkaindex");
    boolean indexExists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
    if(!indexExists) {
        System.out.println("Index doesnt exist");
        CreateIndexRequest request = new CreateIndexRequest("kafkaindex");
        request.settings(Settings.builder()
                .put("index.number_of_shards", 5)
                .put("index.number_of_replicas", 1));
        request.alias(new Alias("FinancialData"));
        CreateIndexResponse createIndexResponse = client.indices()
                .create(request, RequestOptions.DEFAULT);
        if(createIndexResponse.isAcknowledged()) {
            System.out.println("Acknowledged");
            putDataInElasticSearch(consumer,client);
            return;
        } else {
//            logger.info("Error!");
            System.out.println("Not acked");
            return;
        }
    }
    putDataInElasticSearch(consumer,client);
//    while (true) {
//        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
//        for (ConsumerRecord<String, String> record : records) {
//
//
//                logger.info("Key : "+record.key()+" Value : "+ record.value());
//                logger.info("Partition : "+record.partition()+" Offset : "+record.offset());
//
//
////            IndexRequest indexRequest = new IndexRequest("kafka")
////                    .source(record.value(), XContentType.JSON);
////            IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
////            String id = indexResponse.getId();
////            logger.info(id);
//
//
//
//
//
//        }
//    }
}catch (Exception e)
{
    System.out.println("Exception"+e);
    long end = System.nanoTime();
    System.out.println(end - startTime);
}
//        client.close();
    }

    public static KafkaConsumer<String,String> createConsumer(String topic){
        String bootstrapServer = "3.145.10.98:9092";
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
//        String hostname="kafka-example-3164371506.us-east-1.bonsaisearch.net";
//        String username="dz80tuqdj0";
//        String password="oa3qbv3pkb";
//
//        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//        credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(username,password));
//
//        RestClientBuilder builder = RestClient.builder(
//                new HttpHost(hostname,443,"https"))
//                .setHttpClientConfigCallback(new RestClientBuilder
//                        .HttpClientConfigCallback() {
//                    @Override
//                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
//                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
//                    }
//                });
//        RestHighLevelClient client = new RestHighLevelClient(builder);
//        return client;

        RestClientBuilder builder = RestClient.builder(new HttpHost("3.145.81.129",9200,"http"));
        return new RestHighLevelClient(builder);
    }


    private static void putDataInElasticSearch(KafkaConsumer<String,String> consumer, RestHighLevelClient client) throws ParseException,
            IOException {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.key());
                String jsonString = record.value();
                String id_str = extractIdFromJson(record.value());
                logger.info("Key:" + record.key() + " Value:" + record.value());
                logger.info("Partition:" + record.partition() + " Offset:" + record.offset());
                IndexRequest indexRequest = new IndexRequest(
                        "kafkaindex"
                ).source(jsonString, XContentType.JSON).id(id_str);
                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                System.out.println("INDEX RESPONSE : "+indexResponse);
                String id = indexResponse.getId();
                logger.info("ID : "+id);
//                Thread.sleep(1000);
            }
        }
    }


    private static String extractIdFromJson(String json) throws ParseException {
        System.out.println(json);
        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) jsonParser.parse(json);
        Optional<Object> id_str = Optional.ofNullable(jsonObject.get("id_str"));
        return id_str.isPresent() ? id_str.toString() : UUID.randomUUID().toString();
    }
}
