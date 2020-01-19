package com.jw9j.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Properties;

// before run select
public class WindowsWordCount {
    public static void main(String[] args) throws Exception {
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers","localhost:9092");
//        properties.setProperty("zookeeper.connet","localhost:2181");
//        properties.setProperty("group.id","test");
//        获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        指定数据源和
//        web socket
        DataStream<Tuple2<String,Integer>> dataStream = env.socketTextStream("localhost",9999)
                .flatMap(new Splitter())
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        dataStream.writeAsText("./test.csv");
        env.execute("windwos count");
    }
    public static class Splitter implements FlatMapFunction<String,Tuple2<String,Integer>>{
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for(String word:s.split(" ")){
                collector.collect(new Tuple2<String,Integer>(word,1));
            }
        }
    }
}
