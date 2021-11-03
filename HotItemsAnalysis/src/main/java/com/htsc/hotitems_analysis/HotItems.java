package com.htsc.hotitems_analysis;

import com.htsc.hotitems_analysis.beans.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class HotItems {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> readTextFileDS = env.readTextFile("D:\\Codedevelop\\Flink_project\\Userbehavior\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv");

        SingleOutputStreamOperator<UserBehavior> mapDS = readTextFileDS.map(value -> {
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

        KeyedStream<UserBehavior, Long> keyedStream = userBehaviorDS.filter(value -> "pv".equals(value.getBehavior()))
                .keyBy(value -> value.getItemId());

        keyedStream.print();

        env.execute();

    }
}
