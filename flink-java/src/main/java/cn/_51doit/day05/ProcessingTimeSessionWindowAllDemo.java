package cn._51doit.day05;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 不keyBy，直接划分回话窗口，即按照ProcessingTime划分回话窗口
 * 底层调用的是windowAll方法，传入的是ProcessingTime类型的SessionWindow
 *
 * 如果使用的是ProcessingTime，就是当前系统时间 - 进入到该窗口中最后一天数据对于的系统时间 > 指定的时间间隔 ，前面的数据就会形成一个窗口触发
 *
 */
 public class ProcessingTimeSessionWindowAllDemo {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);

        //按照时间间隔划分窗口
        AllWindowedStream<Integer, TimeWindow> windowedStream = nums.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));

        SingleOutputStreamOperator<Integer> res = windowedStream.sum(0);

        res.print();

        env.execute();

    }
}
