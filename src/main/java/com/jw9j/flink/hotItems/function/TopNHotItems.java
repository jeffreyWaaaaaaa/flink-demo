package com.jw9j.flink.hotItems.function;
// 计算某个窗口前名的热门点击商品，key为窗口时间戳，输出为TopN的结果字符串

import com.jw9j.flink.hotItems.model.ItemViewCount;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount,String> {
    private final int topSize;

    public TopNHotItems(int topSize) {
        this.topSize = topSize;
    }
//    存储商品与点击数的状态，待收齐统一窗口的数据后，在触发TopN计算
    private ListState<ItemViewCount> itemViewCountListState;
    @Override
    public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {
//        每条数据都保存到状态中
        itemViewCountListState.add(itemViewCount);
        context.timerService().registerEventTimeTimer(itemViewCount.windowsEnd+1);

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ListStateDescriptor<ItemViewCount> itemViewCountListStateDescriptor = new ListStateDescriptor<ItemViewCount>(
                "item-State-state",
                ItemViewCount.class
        );
        itemViewCountListState = getRuntimeContext().getListState(itemViewCountListStateDescriptor);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
//        获取收到的所有商品点击量
        List<ItemViewCount> allItems = new ArrayList<>();
        for(ItemViewCount item:itemViewCountListState.get()){
            allItems.add(item);
        }
        itemViewCountListState.clear();
        allItems.sort(new Comparator<ItemViewCount>() {
            @Override
            public int compare(ItemViewCount o1, ItemViewCount o2) {
                return (int)(o2.viewCount-o1.viewCount);
            }
        });
//        将排名信息转化成String 便于打印
        StringBuilder result = new StringBuilder();
        result.append("================");
        result.append("时间：").append(new Timestamp(timestamp-1)).append("\n");
        for(int i=0;i<allItems.size() && i< topSize;i++){
            ItemViewCount currentitem = allItems.get(i);
            result.append("No").append(i).append(":")
                    .append(" 商品Id= ").append(currentitem.itemId)
                    .append(" 浏览量=").append(currentitem.viewCount).append("\n");
        }
        result.append("=======\n\n");
        Thread.sleep(1000);
        out.collect(result.toString());
    }

}
