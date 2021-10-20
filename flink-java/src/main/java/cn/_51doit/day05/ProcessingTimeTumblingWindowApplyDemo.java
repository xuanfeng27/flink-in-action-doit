package cn._51doit.day05;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.stream.Collectors;

/**
 * 先keyBy，再按照ProcessingTime划分滚动窗口，然后再调用apply，将窗口内的数据先攒起来，窗口触发后再进行运算
 *
 *
 */
public class ProcessingTimeTumblingWindowApplyDemo {

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

        //先调研keyBy
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndCount.keyBy(t -> t.f0);

        //按照ProcessingTime划分滚动窗口,30秒滚动一次
        //即滚动窗口30秒触发一次，就会产生一次结果
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(30)));

        //spark,5
        //spark,8
        //spark,3
        //spark,6
        //再窗口内，将输入的数据先攒起来，窗口触发后按照单词的次数进行排序（将同一个key的value进行排序）
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = windowedStream.apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {

            /**
             * apply方法在窗口触发时才会调用
             * @param key
             * @param window
             * @param input
             * @param out
             * @throws Exception
             */
            @Override
            public void apply(String key, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
                System.out.println("apply method invoked ~~~~");

//                ArrayList<Tuple2<String, Integer>> lst = new ArrayList<>();
//                for (Tuple2<String, Integer> tp : input) {
//                    lst.add(tp);
//                }
                ArrayList<Tuple2<String, Integer>> lst = (ArrayList<Tuple2<String, Integer>>) input;
                //排序
                lst.sort((a, b) -> Integer.compare(b.f1, a.f1));
                //取出最大的top3
                for (int i = 0; i < Math.min(3, lst.size()); i++) {
                    out.collect(lst.get(i));
                }

            }
        });

        res.print();

        env.execute();

    }
}
