package cn._51doit.day09;

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
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 2.使用ProcessFunction的状态（只能针对与KeyedStream）
 * 在KeyedStream中 使用 KeyedProcessFunction，并且使用状态（KeyedState）
 */
public class KeyedStreamUseKeyedState {

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

        //先keyBy然后调用keyedProcessFunction，再使用keyedState
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            private transient ValueState<Integer> valueState;

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
            public void processElement(Tuple2<String, Integer> in, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                Integer current = in.f1;
                Integer history = valueState.value();
                if (history == null) {
                    history = 0;
                }
                current += history;
                valueState.update(current);
                in.f1 = current;
                out.collect(in);
            }
        });

        res.print();

        env.execute();

    }


}
