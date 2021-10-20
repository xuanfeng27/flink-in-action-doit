package cn._51doit.day05;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * 按照数据的调试划分窗口（数据达到指定的条数，窗口触发）
 *
 * 如果调用windowAll方法得到的操作就是Non-KeyedWindow（没有分区的窗口），该窗口只有1个并行，即一个分区
 * 如果处理大量数据，使用这种NonKeyedWindow就只有一个subtask处理窗口逻辑的数据，效率低，生产环境一般不使用
 *
 */
public class CountWindowAllDemo {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);

        //windowAll，没有分区
        //AllWindowedStream<Integer, GlobalWindow> window = nums.countWindowAll(5);
        AllWindowedStream<Integer, GlobalWindow> window = nums.windowAll(GlobalWindows.create()).trigger(PurgingTrigger.of(CountTrigger.of(5)));

        //将整体当成一个组
        SingleOutputStreamOperator<Integer> res = window.sum(0);

        res.print();

        env.execute();

    }
}
