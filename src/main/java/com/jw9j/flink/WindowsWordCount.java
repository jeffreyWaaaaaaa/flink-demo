package com.jw9j.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WindowsWordCount {
    public static void main(String[] args) throws Exception {
//        获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        指定数据源和
//        web socket
        DataStream<Tuple2<String, Integer>> dataStream = env.socketTextStream("10.126.21.36", 9001)
                .flatMap(new Splitter())
                .keyBy(0)
//                .timeWindow(Time.seconds(5))
                .sum(1);
        dataStream.print();
        env.execute("windwos count");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for (String word : s.split(" ")) {
                collector.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
