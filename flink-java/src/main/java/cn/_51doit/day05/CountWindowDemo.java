package cn._51doit.day05;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * 按照数据的调试划分窗口（数据达到指定的条数，窗口触发）
 * 
 * 先keyby,再划分窗口，这样得到的窗口为keyedWindow,是多并行的，即有多个subtask执行窗口中的逻辑
 *
 * 先keyBy再调研window方法，传入CountTrigger，这里数量为5，即一个分区中一个组中的数据达到5条，这个组单独触发。
 *
 *
 *
 */
public class CountWindowDemo {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        //spark,3
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCount = lines.map(line -> {
            String[] fields = line.split(",");
            String word = fields[0];
            int count = Integer.parseInt(fields[1]);
            return Tuple2.of(word, count);
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndCount.keyBy(t -> t.f0);

        //对keyby之后的KeyedStream划分窗口
        //WindowedStream<Tuple2<String, Integer>, String, GlobalWindow> windowedStream = keyedStream.countWindow(5);
        WindowedStream<Tuple2<String, Integer>, String, GlobalWindow> windowedStream = keyedStream.window(GlobalWindows.create()).trigger(PurgingTrigger.of(CountTrigger.of(5)));



        SingleOutputStreamOperator<Tuple2<String, Integer>> res = windowedStream.sum(1);

        res.print();

        env.execute();

    }
}
