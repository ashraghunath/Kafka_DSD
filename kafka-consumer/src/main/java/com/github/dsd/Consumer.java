package com.github.dsd;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

import static com.github.dsd.ConsumerConstants.*;

public class Consumer {


    static Logger logger = LoggerFactory.getLogger(Consumer.class.getName());

    public static void main(String[] args) {

        long startTime = 0L;
        try {

            RestHighLevelClient client = createClient();
            ElasticSearchService elasticSearchService = new ElasticSearchService();

            startTime = System.nanoTime();
            KafkaConsumer<String, String> consumer = createConsumer(TOPIC);

            GetIndexRequest getIndexRequest = new GetIndexRequest();
            getIndexRequest.indices(ELASTICSEARCH_INDEX);
            boolean indexExists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
            if (!indexExists) {
                System.out.println("Index doesnt exist");
                CreateIndexRequest request = new CreateIndexRequest(ELASTICSEARCH_INDEX);
                request.settings(Settings.builder()
                        .put("index.number_of_shards", 5)
                        .put("index.number_of_replicas", 1));
                request.alias(new Alias(ALIAS));
                CreateIndexResponse createIndexResponse = client.indices()
                        .create(request, RequestOptions.DEFAULT);
                if (createIndexResponse.isAcknowledged()) {
                    logger.info("Acknowledged");
                    elasticSearchService.putDataInElasticSearch(consumer, client);
                    return;
                } else {
                    logger.info("Not acknowledged");
                    return;
                }
            }
            //todo twice?
            elasticSearchService.putDataInElasticSearch(consumer, client);

        } catch (Exception e) {
            System.out.println("Exception" + e);
            long end = System.nanoTime();
            System.out.println(end - startTime);
        }
    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {

        String bootstrapServer = BOOTSTRAP_SERVER_PUBLIC_IP + ":" + BOOTSTRAP_SERVER_PUBLIC_IP_PORT;
        String groupId = CONSUMER_GROUP_ID;

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList(topic));
        return kafkaConsumer;

    }

    public static RestHighLevelClient createClient() {
        RestClientBuilder builder = RestClient.builder(new HttpHost(ELASTICSEARCH_IP, ELASTICSEARCH_IP_PORT, "http"));
        return new RestHighLevelClient(builder);
    }

}
