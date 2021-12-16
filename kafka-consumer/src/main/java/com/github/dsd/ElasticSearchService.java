package com.github.dsd;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

import static com.github.dsd.ConsumerConstants.ELASTICSEARCH_INDEX;

public class ElasticSearchService {


    public ElasticSearchService() {
    }

    static Logger logger = LoggerFactory.getLogger(Consumer.class.getName());


    public void putDataInElasticSearch(KafkaConsumer<String, String> consumer, RestHighLevelClient client) throws ParseException,
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
                        ELASTICSEARCH_INDEX
                ).source(jsonString, XContentType.JSON).id(id_str);
                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                System.out.println("INDEX RESPONSE : " + indexResponse);
                String id = indexResponse.getId();
                logger.info("ID : " + id);
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
