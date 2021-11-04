package com.htsc.hotitems_analysis;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class KafkaProducerUtile {
    public static void main(String[] args) throws IOException {
        writeToKafka("hotitems");
    }

    public static void writeToKafka(String topic) throws IOException {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        BufferedReader bufferedReader = new BufferedReader(new FileReader("D:\\Codedevelop\\Flink_project\\Userbehavior\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv"));

        String line;

        while ((line = bufferedReader.readLine()) != null){
            ProducerRecord<String, String> produceRecord = new ProducerRecord<>(topic, line);
            kafkaProducer.send(produceRecord);
        }
        bufferedReader.close();
        kafkaProducer.close();
    }
}
