package com.walmart.ca.catalog.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.walmart.ca.catalog.model.Employee;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

@Service
public class ProductIngestionService implements Serializable {

    @Autowired
    private SparkConf sparkConf;

    /*@Autowired
    private Map<String,Object> commonKafkaParams;*/

    @Autowired
    private ObjectMapper objectMapper;

    @Value("${spark.kafka.sku-index.topic}")
    private String skuIndexTopic;

    @Value("${spark.kafka.sku-index.topic}")
    private String skuGroupId;

    public void initializeProductIngestion() throws InterruptedException {
        System.out.println("####### start");
        Map<String, Object> commonKafkaParams = new HashMap<>();
        commonKafkaParams.put("bootstrap.servers", "localhost:9092");
        commonKafkaParams.put("key.deserializer", StringDeserializer.class);
        commonKafkaParams.put("value.deserializer", StringDeserializer.class);
        commonKafkaParams.put("auto.offset.reset", "latest");
        commonKafkaParams.put("enable.auto.commit", false);

        Map<String, Object> kafkaParams = new HashMap<>(commonKafkaParams);
        kafkaParams.put("group.id",skuGroupId);

        JavaStreamingContext streamingContext = new JavaStreamingContext(
                sparkConf, Durations.seconds(5));

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(List.of(skuIndexTopic), kafkaParams));

        stream.foreachRDD((allSkuRdd,time) ->{
            OffsetRange[] offsetRanges = ((HasOffsetRanges) allSkuRdd.rdd()).offsetRanges();
            process(allSkuRdd);
            ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
        } );

        streamingContext.start();
        streamingContext.awaitTermination();
    }

    private void process(JavaRDD<ConsumerRecord<String, String>> allSkuRdd) {
        allSkuRdd.foreach(record -> System.out.println("################# : "+record.value()));
        JavaRDD<Employee> stringJavaRDD = allSkuRdd.filter(r -> !r.value().isEmpty())
                .map(ConsumerRecord::value)
                .map(empJson -> objectMapper.convertValue(empJson,Employee.class));
        stringJavaRDD.foreach(record -> System.out.println("################# v : "+record));

        javaFunctions(stringJavaRDD).writerBuilder("ca_catalog_testdb", "employee", mapToRow(Employee.class)).saveToCassandra();
        System.out.println("################# Done saving: ");
    }
}
