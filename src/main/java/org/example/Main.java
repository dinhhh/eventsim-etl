package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("Event sim ETL")
                .master("local")
                .getOrCreate();
        System.out.println("Hello world!");
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group-client-id");
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "eventsim-load-client");
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, Event.class);

        Consumer eventsimConsumer = new KafkaConsumer(consumerProps);
        eventsimConsumer.subscribe(Arrays.asList("eventsim"));

        ObjectMapper objectMapper = new ObjectMapper();
        while (true) {
            ConsumerRecords<String, Event> records = eventsimConsumer.poll(Duration.ZERO);
            records.forEach(r -> {
                String content;
                try {
                    content = objectMapper.writeValueAsString(r.value());
                    System.out.printf("Start write content %s%n", content);
                    writeToParquet(sparkSession, r.value());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private static void showDataframe(SparkSession sparkSession) {
        Dataset<Row> df = sparkSession.read().parquet("hdfs://localhost:9000/20-05-2023-data.parquet");
        df.show();
    }

    private static void writeToJson(String content) {
        try {
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS", "hdfs://localhost:9000");
            configuration.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
            configuration.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
            configuration.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
            FileSystem fileSystem = FileSystem.get(configuration);
            // Create a path
            LocalDateTime now = LocalDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy");
            String fileName = String.format("%s-data.json", now.format(formatter));

            Path hdfsWritePath = new Path("/home/dinh/hadoop/dfsdata/datanode/current/" + fileName);

            FSDataOutputStream fsDataOutputStream;
            if (!fileSystem.exists(hdfsWritePath)) {
                fsDataOutputStream = fileSystem.create(hdfsWritePath, true);
                fileSystem.setReplication(hdfsWritePath, (short) 1);
                System.out.println("Create new file");
            } else {
                System.out.println("Append to existing file");
                fsDataOutputStream = fileSystem.append(hdfsWritePath);
            }
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
            bufferedWriter.write(content);
            bufferedWriter.newLine();
            bufferedWriter.close();
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void writeToParquet(SparkSession sparkSession, Event event) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
        configuration.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        configuration.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        configuration.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        FileSystem fileSystem = FileSystem.get(configuration);

        // Create a path
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy");
        String fileName = String.format("hdfs://localhost:9000/%s-data.parquet", now.format(formatter));

        Path hdfsWritePath = new Path(fileName);

        List<Event> events = new ArrayList<>();
        events.add(event);

        Dataset<Row> df = sparkSession.createDataFrame(events, Event.class);
        if (!fileSystem.exists(hdfsWritePath)){
            df.write().parquet(fileName);
        } else {
            df.write().mode(SaveMode.Append).parquet(fileName);
        }
    }

}