package com.jw9j.flink.sqlSource;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReadSoure {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<ClassRoom> dataStream = env.addSource(new SoureFromMysql());

        dataStream.print();
        dataStream.addSink(new SinkToMysql());
//        System.out.println(dataStream.print());
        env.execute();
    }
}
