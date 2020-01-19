package com.jw9j.flink.etltest;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReadSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<OrderDetail> dataStream = env.addSource(new SourceFromSql());
        dataStream.addSink(new SinkToSql());
        env.execute();
        System.out.println("导入结束！");
    }
}
