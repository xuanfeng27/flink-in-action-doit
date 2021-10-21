package cn._51doit.day06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

/**
 * 自己写一个Flink的程序，将中间结果计算出来，不使用Flink的状态API
 *
 * 解决各自累加各自key的次数
 *
 */
public class MyKeyedStateDemo2 {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        //如果开启了checkpoint，那么job的默认重启策略为无限重启（Integer.MAX_VALUE）
        env.enableCheckpointing(10000);

        //spark,2
        //hive,1
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                String word = fields[0];
                if(word.startsWith("error")) {
                    throw new RuntimeException("数据出问题了！");
                }
                int count = Integer.parseInt(fields[1]);
                return Tuple2.of(word, count);
            }
        });

        //sum底层调用的是reduce方法，再StreamGroupedReduceOperator的processElement使用和更新了状态
        //tpStream.keyBy(t -> t.f0).sum(1).print();

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = tpStream.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> res = keyedStream.map(new MyReduceFunction2());

        res.print();

        env.execute();

    }

    /**
     * 在同一个subtask中，各自累加各自key的value
     */
    private static class MyReduceFunction2 extends RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

        private Map<String, Integer> wc = new HashMap<>();

        //同一个分区（同一个subtask）来一条数据会调用一次map方法
        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Integer> in) throws Exception {
            String word = in.f0;
            Integer currentCount = in.f1;
            Integer historyCount = wc.get(word);
            if (historyCount == null) {
                historyCount = 0;
            }
            int totalCount = historyCount + currentCount;
            //将数据更新到map中
            wc.put(word, totalCount);
            //输出数据
            in.f1 = totalCount;
            return in;
        }
    }

}
