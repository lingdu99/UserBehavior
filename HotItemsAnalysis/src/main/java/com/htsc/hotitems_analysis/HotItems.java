package com.htsc.hotitems_analysis;

import com.htsc.hotitems_analysis.beans.ItemViewCount;
import com.htsc.hotitems_analysis.beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
/*
需求说明：
每隔5分钟输出最近一小时内点击量最多的前N个商品。将这个需求进行分解我们大概要做这么几件事情：
抽取出业务时间戳，告诉Flink框架基于业务时间做窗口
过滤出点击行为数据
按一小时的窗口大小，每5分钟统计一次，做滑动窗口聚合（Sliding Window）
按每个窗口聚合，输出每个窗口中点击量前N名的商品
 */

public class HotItems {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //文件读取数据
        //DataStreamSource<String> readTextFileDS = env.readTextFile("D:\\Codedevelop\\Flink_project\\Userbehavior\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv");
        //DataStreamSource<String> readSockeDS = env.socketTextStream("hadoop102", 9999);
        Properties properties = new Properties();
        properties.setProperty("bootstrap","localhost:9092");
        properties.setProperty("group.id","consumer");
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset","latest");

        DataStreamSource<String> dataKafkaDS = env.addSource(new FlinkKafkaConsumer<String>("hotitems", new SimpleStringSchema(), properties));

        SingleOutputStreamOperator<UserBehavior> mapDS = dataKafkaDS.map(value -> {
            String[] split = value.split(",");
            return new UserBehavior(new Long(split[0]), new Long(split[1]), new Integer(split[2]), split[3], new Long(split[4]));
        });

        WatermarkStrategy<UserBehavior> userBehaviorWatermarkStrategy = WatermarkStrategy.<UserBehavior>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
            @Override
            public long extractTimestamp(UserBehavior userBehavior, long l) {
                return userBehavior.getTimestamp() * 1000L;
            }
        });

        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = mapDS.assignTimestampsAndWatermarks(userBehaviorWatermarkStrategy);

        KeyedStream<Tuple2<Long,Integer>, Long> keyedStream = userBehaviorDS.filter(value -> "pv".equals(value.getBehavior())).map(new MapFunction<UserBehavior, Tuple2<Long,Integer>>() {
            @Override
            public Tuple2<Long, Integer> map(UserBehavior userBehavior) throws Exception {
                return new Tuple2<>(userBehavior.getItemId(),1);
            }
        })
                .keyBy(value -> value.f0);

        SingleOutputStreamOperator<ItemViewCount> aggregate = keyedStream.window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(5)))
                .aggregate(new ItemCountAggFunc(),new ItemCountWindowFunc());

        SingleOutputStreamOperator<String> resultStream = aggregate.keyBy(value -> value.getWindowEnd()).process(new TopNHotItems(5));


        resultStream.print();

        env.execute();

    }

    public static class ItemCountAggFunc implements AggregateFunction<Tuple2<Long,Integer>,Integer,Integer> {

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple2<Long, Integer> value, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }

    public static class ItemCountWindowFunc implements WindowFunction<Integer, ItemViewCount,Long, TimeWindow>{

        @Override
        public void apply(Long aLong, TimeWindow timeWindow, Iterable<Integer> input, Collector<ItemViewCount> collector) throws Exception {
            Long count = new Long(input.iterator().next());

            collector.collect(new ItemViewCount(aLong,timeWindow.getEnd(),count));
        }
    }

    public static class TopNHotItems extends KeyedProcessFunction<Long,ItemViewCount,String>{

        private ListState<ItemViewCount> listState;

        private Integer topSize;

        public TopNHotItems(Integer topSize){
            this.topSize = topSize;
        }

        @Override
        public void open(Configuration configuration){
            listState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("list-state", ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {
            //将数据存入状态
            listState.add(itemViewCount);

            //定时器

            context.timerService().registerEventTimeTimer(itemViewCount.getWindowEnd() + 1);
//            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//            context.timerService().registerEventTimeTimer(simpleDateFormat.parse(itemViewCount.getWindowEnd().toString()).getTime()+1000L);

        }

        @Override
        public void onTimer(long timestamp,OnTimerContext context,Collector<String> out) throws Exception {
            //
            Iterator<ItemViewCount> iterator = listState.get().iterator();

            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(iterator);

            itemViewCounts.sort(((o1, o2) -> o2.getCount().intValue() - o1.getCount().intValue()));

            StringBuilder sb = new StringBuilder();

            StringBuilder append = sb.append(" ================= ").append(new Timestamp(timestamp - 1000L))
                    .append(" =============== ").append("\n");

            for (int i = 0 ; i < Math.min(topSize,itemViewCounts.size());i++){
                ItemViewCount itemViewCount = itemViewCounts.get(i);

                sb.append("  Top  ").append(i+1);
                sb.append("  ItemId:  ").append(itemViewCount.getItemId());
                sb.append("  Count:  ").append(itemViewCount.getCount());
                sb.append("\n");
            }

            sb.append(" ============= ").append(new Timestamp(timestamp - 1000L)).append("\n").append("\n");

            // 清空数据，并输出数据

            listState.clear();

            out.collect(sb.toString());

            Thread.sleep(2000);

        }
    }
}
