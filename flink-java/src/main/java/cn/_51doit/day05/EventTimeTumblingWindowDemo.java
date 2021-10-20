package cn._51doit.day05;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 先keyBy，再按照EventTime划分滚动窗口
 *
 *
 */
public class EventTimeTumblingWindowDemo {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        //使用老的API
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //1634720400000,spark,3
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //提取数据中的时间
        //数据还是1634720400000,spark,3这个样子，只不过里面有了水位线（触发EventTime类型窗口执行的信号机制）
        //设置窗口延迟触发的时间
        SingleOutputStreamOperator<String> streamWithWaterMark = lines.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(String line) {
                String[] fields = line.split(",");
                return Long.parseLong(fields[0]);
            }
        });

        //水位线 = 窗口当前分区中最大的EventTime - 窗口延迟时间
        //窗口长度为10秒
        //第一次输入1634720403000,spark,3  那么就确定了第一个窗口的起始时间和结束时间 即 [1634720403000,1634720410000) 或 [1634720403000,1634720409999]
        //水位线1 = 1634720403000 - 0 = 1634720403000
        //当输入1634720409998,hive,2
        //水位线2 = 1634720409998 - 0 = 1634720409998
        //如果输入1634720409999,spark,1
        //水位线3 = 1634720409999 - 0 = 1634720409999
        //窗口触发的条件：水位线 >= 窗口的结束时间，那么窗口就触发
        //水位线3 >= 1634720409999(第一个窗口的结束时间)
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCount = streamWithWaterMark.map(line -> {
            //1634720403000,spark,3
            String[] fields = line.split(",");
            String word = fields[1];
            int count = Integer.parseInt(fields[2]);
            return Tuple2.of(word, count);
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        //先调研keyBy
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndCount.keyBy(t -> t.f0);

        //划分EventTime类型的窗口
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        //调用window function
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = windowedStream.sum(1);

        res.print();

        env.execute();

    }
}
