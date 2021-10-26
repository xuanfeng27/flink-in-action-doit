package cn._51doit.day09;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

/**
 * 使用测流输出获取窗口迟到的数据
 */
public class GetWindowLateData {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        //使用老的API
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //5000,spark,3
        //7000,spark,3
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> tpStream = lines.map(new MapFunction<String, Tuple3<Long, String, Integer>>() {
            @Override
            public Tuple3<Long, String, Integer> map(String line) throws Exception {
                String[] fields = line.split(",");
                long ts = Long.parseLong(fields[0]);
                String word = fields[1];
                int count = Integer.parseInt(fields[2]);
                return Tuple3.of(ts, word, count);
            }
        }).setParallelism(1);

        //提取数据中的时间
        //（触发EventTime类型窗口执行的信号机制）
        //设置窗口延迟触发的时间为2秒
        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> wordAndCountWithMaterMark = tpStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Long, String, Integer>>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(Tuple3<Long, String, Integer> tp) {
                return tp.f0;
            }
        });


        //先调研keyBy
        KeyedStream<Tuple3<Long, String, Integer>, String> keyedStream = wordAndCountWithMaterMark.keyBy(t -> t.f1);

        //默认情况，迟到的数据会被丢弃
        //先定义一个tag，给迟到的数据打上该标签
        OutputTag<Tuple3<Long, String, Integer>> lateDateTag = new OutputTag<Tuple3<Long, String, Integer>>("late-date") {};

        //划分EventTime类型的窗口
        WindowedStream<Tuple3<Long, String, Integer>, String, TimeWindow> windowedStream = keyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .sideOutputLateData(lateDateTag); //将窗口迟到的数据打上标签


        //调用window function
        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> res = windowedStream.sum(2);

        //获取迟到的数据，获取打标签的数据(主流中的按照是否打标签，分为2种，一种是未打标签的，一种是打标签的)
        DataStream<Tuple3<Long, String, Integer>> lateDataStream = res.getSideOutput(lateDateTag);

        lateDataStream.print("late-data ");

        res.print("normal ");

        env.execute();

    }
}
