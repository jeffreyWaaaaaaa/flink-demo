package com.jw9j.flink.hotItems;

import com.jw9j.flink.hotItems.function.CountAgg;
import com.jw9j.flink.hotItems.function.TopNHotItems;
import com.jw9j.flink.hotItems.function.WindowResultFunction;
import com.jw9j.flink.hotItems.model.ItemViewCount;
import com.jw9j.flink.hotItems.model.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

public class HotItems {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        UserBehavior.csv 本地文件路径
        URL fileUrl = HotItems.class.getClassLoader().getResource("UserBehavior.csv");
        Path path = Path.fromLocalFile(new File(fileUrl.toURI()));
//        抽取UserBehavior的TypeInformation, 是一个PojoTypeInfo
        PojoTypeInfo<UserBehavior> pojoTypeInfo =(PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);
//        指定反射取出字段的顺序
        String[] fileOrder =  new String[]{"userId","itemId","categoryId","behavior","timestamp"};
        PojoCsvInputFormat<UserBehavior> csvInput = new PojoCsvInputFormat<>(path,pojoTypeInfo,fileOrder);
//        创建输入源
        DataStream<UserBehavior> dataStream = env.createInput(csvInput,pojoTypeInfo);
//        按照Eventtime 模式处理数据
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        时间戳抽取并生成Watermark
        DataStream<UserBehavior> timeData = dataStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
//                将数据转换成毫秒
                return userBehavior.timestamp*1000;
            }
        });
//        统计点击量最多的前N个商品
        DataStream<UserBehavior> pvData = timeData.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior userBehavior) throws Exception {
//                过滤出只有点击的数据
                return userBehavior.behavior.equals("pv");
            }
        });
//        窗口统计点击量
        DataStreamSink<String> windowedData = pvData.keyBy("itemId")
                .timeWindow(Time.minutes(60),Time.minutes(5))
                .aggregate(new CountAgg(),new WindowResultFunction())
                .keyBy("windowsEnd")
                .process(new TopNHotItems(10))
                .print();
        env.execute("hot items Job");
    }

}
