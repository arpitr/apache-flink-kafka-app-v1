package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.Properties;

public class OrderSummaryJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "flink-order-group");

        // 1. Read from Kafka
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "order-data",
                new SimpleStringSchema(),
                props
        );
        consumer.setStartFromEarliest();

        DataStream<String> input = env.addSource(consumer);

        // 2. Parse JSON and extract order_amount and order_id
        DataStream<Tuple2<Double, Integer>> amounts = input
            .map(line -> {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode root = mapper.readTree(line);
                JsonNode payload = root.get("payload");
                double amount = payload.get("order_amount").asDouble();
                int count = payload.has("order_id") ? 1 : 0;
                Tuple2<Double, Integer> tuple = Tuple2.of(amount, count);
                System.out.println("Parsed Tuple2: " + tuple);
                return tuple;
            })
            .returns(TypeInformation.of(new TypeHint<Tuple2<Double, Integer>>() {}));
        
        amounts.print();

        // 3. Windowed sum and count (10 min tumbling window)
        DataStream<String> summary = amounts
            .timeWindowAll(Time.minutes(10))
            .reduce((a, b) -> Tuple2.of(a.f0 + b.f0, a.f1 + b.f1))
            .map(t -> String.format("{\"window_sum\":%.2f,\"window_count\":%d}", t.f0, t.f1));

        summary.print();
        // 4. Write to Kafka
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
                "order-data-summary",
                new SimpleStringSchema(),
                props
        );
        summary.addSink(producer);

        env.execute("Order Data Summary Job");
    }
}