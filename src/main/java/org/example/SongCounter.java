package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Collections;
import java.util.Properties;

public class SongCounter {

    public static void main(String[] args) throws InterruptedException {
        String topic = "eventsim";
        String outputTopic = "eventsim-song-count";
        StreamsBuilder builder = new StreamsBuilder();

        Serializer<JsonNode> serializer = new JsonSerializer();
        Deserializer<JsonNode> deserializer = new JsonDeserializer();
        org.apache.kafka.common.serialization.Serde<JsonNode> serde = Serdes.serdeFrom(serializer, deserializer);

        Consumed<String, JsonNode> consumed = Consumed.with(Serdes.String(), serde);
        KStream<String, JsonNode> eventsKStream = builder.stream(topic, consumed);

        KTable<String, Long> kTable = eventsKStream
                .peek((key, value) -> System.out.printf("Receive (key, value): (%s, %s)%n", key, value.get("song").textValue()))
                .flatMapValues(value -> Collections.singletonList(value.get("song").textValue()))
                .groupBy((key, song) -> song)
                .count();
        kTable.toStream()
                .foreach((song, count) -> System.out.println("song: " + song + " -> " + count));
        kTable.toStream()
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "application");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Topology topology = builder.build();
        topology.describe();
        KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);
        streams.start();
        Thread.sleep(15000L);
        streams.close();
    }

}
