package com.project.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;

public class FlinkJob {

    public static void executeFlinkJob(StreamExecutionEnvironment env) throws Exception {
        // Configure the Kafka source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")  // Kafka broker address
                .setTopics("stock-data")               // Kafka topic to consume data from
                .setGroupId("com.project.flink")       // Consumer group ID for managing offsets
                .setStartingOffsets(OffsetsInitializer.latest()) // Start consuming from the latest offsets
                .setValueOnlyDeserializer(new SimpleStringSchema()) // Deserialize the Kafka message as a plain string
                .build();

        // Create a DataStream from the Kafka source
        DataStream<String> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.forMonotonousTimestamps(), // Assign watermarks for event time processing
                "Kafka Source"                               // Name of the source for Flink job monitoring
        );

        // Deserialize each JSON message into a POJO (Plain Old Java Object) of type StockData
        DataStream<StockData> stockDataStream = kafkaStream.map(value -> {
            ObjectMapper objectMapper = new ObjectMapper();  // ObjectMapper to handle JSON parsing
            return objectMapper.readValue(value, StockData.class); // Deserialize JSON to StockData object
        });

        // Apply a sliding window operation
        DataStream<StockData> windowedStream = stockDataStream
                .keyBy(StockData::getSymbol) // Group data by the stock symbol
                .window(SlidingEventTimeWindows.of(Duration.ofMinutes(10), Duration.ofMinutes(1))) 
                // Define a sliding window of 10 minutes with a slide interval of 1 minute
                .reduce((value1, value2) -> {
                    // Aggregate the data: calculate the average price and keep the latest timestamp
                    return new StockData(
                            value1.getSymbol(), // Stock symbol
                            (value1.getPrice() + value2.getPrice()) / 2, // Calculate the average price
                            Math.max(value1.getTimestamp(), value2.getTimestamp()) // Take the latest timestamp
                    );
                });

        // Print the results to the console for debugging purposes
        windowedStream.print();

        // Execute the Flink job pipeline
        env.execute("Flink Kafka Sliding Window ");
    }
}
