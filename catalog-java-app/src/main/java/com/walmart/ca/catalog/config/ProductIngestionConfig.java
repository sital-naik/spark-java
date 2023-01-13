package com.walmart.ca.catalog.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;

//import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class ProductIngestionConfig {

    @Value("${spark.cores.max}")
    private String maxSparkCores;

    @Value("${spark.cassandra.connection.backend.host}")
    private String cassandraHosts;

    @Value("${spark.cassandra.connection.backend.port}")
    private String cassandraPort;

    @Value("${spark.cassandra.connection.backend.timeout-ms}")
    private String timeoutMs;

    @Value("${spark.cassandra.connection.backend.username}")
    private String username;

    @Value("${spark.cassandra.connection.backend.username}")
    private String password;

    @Value("${spark.kafka.brokers}")
    private String kafkaBrokers;

    @Value("${spark.kafka.sku-index.topic}")
    private String skuIndexTopic;

    @Bean
    public SparkConf getSparkConf(){
        return new SparkConf (true)
                //setting spark specific configurations
                .setMaster ("local" )
                .setAppName("abc")
                .set ("spark.cores.max", maxSparkCores )
                .set ("spark.scheduler.mode", "FAIR")
                .set ("spark.streaming.ui.retainedBatches", String.valueOf (200) )
                .set ("spark.streaming.backpressure.enabled", String.valueOf (true) )
                .set ("spark.streaming.backpressure.initialRate", String.valueOf (200) )
                .set ("spark.ui.retainedStages", String.valueOf (200) )
                .set ("spark.ui.retainedTasks", String.valueOf (200) )
                // cassandra specific settings
                .set("spark.cassandra.connection.host", cassandraHosts)
                .set("spark.cassandra.connection.port", cassandraPort)
                .set("spark.cassandra.connection.timeout_ms", timeoutMs)
                .set("spark.cassandra.auth.username", username)
                .set("spark.cassandra.auth.password", password);
    }

    @Bean("commonKafkaParams")
    public Map<String,Object> getCommonKafkaParams(){
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", kafkaBrokers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("auto.offset.reset", "latest");
        //kafkaParams.put("enable.auto.commit", false);
        return kafkaParams;
    }

    @Bean
    public ObjectMapper getObjectMapper() {
       /* return new Jackson2ObjectMapperBuilder()
                //.serializers(new LocalDateTimeSerializer(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
                .serializationInclusion(JsonInclude.Include.NON_NULL).build();*/
        return new ObjectMapper();
    }
}
