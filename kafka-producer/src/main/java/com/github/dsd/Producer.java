package com.github.dsd;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Properties;

import static com.github.dsd.ProducerConstants.*;

public class Producer {


    public static void main(String[] args) {
        new Producer().run();
    }

    public void run() {
        Gson gson = new Gson();

        File folder = new File(PATH_NAME);
        File[] listOfFiles = folder.listFiles();


        KafkaProducer<String, String> producer = createKafkaProducer();

        String path = null;
        for (File file : listOfFiles) {
            if (file.isFile()) {
                path = file.getAbsolutePath();
                BufferedReader bufferedReader = null;
                try {
                    bufferedReader = new BufferedReader(new FileReader(path));
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }


                Object o = gson.fromJson(bufferedReader, Object.class);
                String original = gson.toJson(o);

                ProducerRecord producerRecord = new ProducerRecord(TOPIC, null, original);

                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null)
                            e.printStackTrace();
                    }
                });
            }
            try {
                Thread.sleep(15000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.flush();
        producer.close();

    }

    public static KafkaProducer<String, String> createKafkaProducer() {

        final String bootstrapServer = BOOTSTRAP_SERVER_PUBLIC_IP + ":" + BOOTSTRAP_SERVER_PUBLIC_IP_PORT;

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");


        //Compression is helpful when batch is bigger, improves performance (less latency, less network bandwidth).
        //control linger.ms and batch.size to tweak batch size
        //for high throughput producer
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_TYPE);
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); //32kb; default is 16kb
        KafkaProducer producer = new KafkaProducer(properties);
        return producer;
    }


}
