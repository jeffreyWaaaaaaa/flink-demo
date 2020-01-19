package com.jw9j.flink.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import sun.awt.SunHints;

import java.util.Properties;

public class WordCount {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","localhost:9092");
        properties.put("zookeeper.connect","localhost:2181");
        properties.put("group.id","test");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer011<String>("test",new SimpleStringSchema(),properties));
        DataStream<Tuple2<String,Integer>> counts = messageStream.flatMap(new LineSplitter()).keyBy(0).sum(1);
        counts.print();
        env.execute("wordCount from data");
    }
}
