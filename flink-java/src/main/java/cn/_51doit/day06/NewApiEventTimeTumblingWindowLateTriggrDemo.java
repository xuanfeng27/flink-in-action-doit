package cn._51doit.day06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

/**
 * 先keyBy，再按照EventTime划分滚动窗口
 * 设置延迟时间大于0
 * <p>
 * WaterMark = 产生WaterMark对应的DataStream每个分区最大的EventTime - 延迟时间
 * <p>
 * 使用心得API提取EventTime生成WaterMark
 */
public class NewApiEventTimeTumblingWindowLateTriggrDemo {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);


        //5000,spark,3
        //7000,spark,3
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> streamWithWaterMark = lines
                .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                            //第一个参数 5000,spark,3
                            @Override
                            public long extractTimestamp(String line, long l) {
                                String[] fields = line.split(",");
                                return Long.parseLong(fields[0]);
                            }
                        }));

        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = streamWithWaterMark.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String line) throws Exception {
                String[] fields = line.split(",");
                return Tuple2.of(fields[1], Integer.parseInt(fields[2]));
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = tpStream.keyBy(t -> t.f0);

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        SingleOutputStreamOperator<Tuple2<String, Integer>> res = windowedStream.sum(1);

        res.print();

        env.execute();

    }
}
