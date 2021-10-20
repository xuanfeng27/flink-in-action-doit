package cn._51doit.day05;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 不keyBy，直接划分滑动窗口，即按照ProcessingTime划分滑动窗口
 * 底层调用的是windowAll方法，传入的是ProcessingTime类型的窗口
 *
 * 调用的是windowAll方法得到是的nonKeyedWindow，window以及window function算子对应的DataStream并行度为1
 */
public class ProcessingTimeSlidingWindowAllDemo {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);

        //第一个时间为窗口长度
        //第二个时间为滑动步长
        AllWindowedStream<Integer, TimeWindow> windowedStream = nums.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)));

        SingleOutputStreamOperator<Integer> res = windowedStream.sum(0);

        res.print();


        env.execute();

    }
}
