package cn._51doit.day06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
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

/**
 * 先keyBy，再按照EventTime划分滚动窗口
 * 设置延迟时间大于0
 *
 * WaterMark = 产生WaterMark对应的DataStream每个分区最大的EventTime - 延迟时间
 *
 */
public class EventTimeTumblingWindowLateTriggerDemo {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        //生成WaterMark的时间周期
        env.getConfig().setAutoWatermarkInterval(200);

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
        });

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

        //划分EventTime类型的窗口
        WindowedStream<Tuple3<Long, String, Integer>, String, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        //调用window function
        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> res = windowedStream.sum(2);

        res.print();

        env.execute();

    }
}
