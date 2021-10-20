package cn._51doit.day05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 不keyBy，直接划分滚动窗口，即按照EventTime划分滚动窗口
 * 底层调用的是windowAll方法，传入的是EventTime类型的窗口
 *
 * 调用的是windowAll方法得到是的nonKeyedWindow，window以及window function算子对应的DataStream并行度为1
 */
public class EventTimeTumblingWindowAllDemo {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        //使用老版本的API(设置时间特征，使用EventTime作为时间标准)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        //1634720103000,1
        //1634720104000,2
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //提取数据中所携带的时间，并且转成long类型
        //assignTimestampsAndWatermarks仅会提取数据中的时间字段，不会改变数据的样子
        //assignTimestampsAndWatermarks返回的dataStream还是原来的样子1634720104000,2
        SingleOutputStreamOperator<String> dataStreamWithWaterMark = lines.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(String line) {
                String[] fields = line.split(",");
                return Long.parseLong(fields[0]);
            }
        });

        //1634720103000,1  ->  1
        SingleOutputStreamOperator<Integer> nums = dataStreamWithWaterMark.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String line) throws Exception {
                String[] fields = line.split(",");
                return Integer.parseInt(fields[1]);
            }
        });

        //1634720103000 - 1634720103000 % 5000 = 1634720100000
        //1634720103000 - 1634720103000 % 5000 + 5000 = 1634720105000
        //Flink窗口的时间精确到毫秒
        //第一次输入的数据是1634720103000，flink eventtime类型的窗口，起始时间、技术时间，必须是窗口长度的整数倍
        //窗口的起始时间：1634720100000
        //窗口的结束时间：1634720105000
        //Flink的窗口区间是前闭后开的[1634720100000, 1634720105000)
        //[1634720105000, 1634720110000) 或 [1634720105000, 1634720109999]
        AllWindowedStream<Integer, TimeWindow> windowedStream = nums.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)));

        SingleOutputStreamOperator<Integer> res = windowedStream.sum(0);

        res.print();

        env.execute();

    }
}
