package cn._51doit.day06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

/**
 * 使用Flink的KeyedState进行编程
 *
 * KeyedState中有多种类型，ValueState就是其中一种
 *
 */
public class ValueStateDemo {

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

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = tpStream.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> res = keyedStream.map(new ValueStateReduceFunction());

        res.print();

        env.execute();

    }

    /**
     * 使用ValueState
     */
    private static class ValueStateReduceFunction extends RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

        private ValueState<Integer> valueState;

        //open方法在map方法执行之前一定会调用一次
        //初始化恢复状态
        @Override
        public void open(Configuration parameters) throws Exception {
            //使用状态的步骤
            //1.定义状态描述器（状态的类型，状态的名称）
            ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("count-state", Integer.class);
            //2.根据状态描述器初始化或恢复状态
            valueState = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Integer> in) throws Exception {
            //String word = in.f0;
            Integer count = in.f1;
            //根据单词找到历史次数(内部使用了keyContext获取当前的单词)
            Integer historyCount = valueState.value();
            if (historyCount == null) {
                historyCount = 0;
            }
            //累加
            int totalCount = historyCount + count;
            //更新状态(内部会使用keyContext将对应单词的次数进行更新)
            valueState.update(totalCount);
            //输出结果
            in.f1 = totalCount;
            return in;
        }
    }

}
