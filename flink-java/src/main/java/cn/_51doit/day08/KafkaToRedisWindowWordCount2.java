package cn._51doit.day08;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * 原来是先调用reduce方法（窗口内增量聚合），再调用windowFunction和历史数据进行聚合
 * reduce方法的缺点：输入和输出数据的类型必须一致
 *
 */
public class KafkaToRedisWindowWordCount2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //开启checkpoint，那就把kafka的偏移量保存到状态中了，checkpoint时会将状态持久化到statebackend中
        env.enableCheckpointing(10000);
        //任务cancel保留外部存储checkpoint
        //如果不设置该属性DELETE_ON_CANCELLATION（job被cancel后，会删除外部的checkpoint数据）
        //一定要加上这是属性RETAIN_ON_CANCELLATION(job被cancel后，保留外部的checkpoint数据)
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置statebackend(设置状态存储的后端)
        //env.setStateBackend(new FsStateBackend("hdfs://node-1.51doit.cn:9000/chk26"));

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "node-1.51doit.cn:9092,node-2.51doit.cn:9092,node-3.51doit.cn:9092");
        properties.setProperty("group.id", "test999");
        properties.setProperty("auto.offset.reset", "earliest"); //如果没有记录历史偏移量就从头读

        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(
                "wc",
                new SimpleStringSchema(),
                properties
        );
        //不将偏移量写入到kafka特殊的topic中
        flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(false);

        //spark hadoop flink flink
        DataStreamSource<String> lines = env.addSource(flinkKafkaConsumer);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndOne.keyBy(t -> t.f0);
        //划分窗口(processingTime类型的滚动窗口)
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));
        //如果直接对窗口进行聚合，那么只会对当前窗口中的数据进行增量聚合，不会聚合历史数据
        //SingleOutputStreamOperator<Tuple2<String, Integer>> res = windowedStream.sum(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> res = windowedStream.aggregate(new MyAggregateFunction(), new MyAggWindowFunction());

        res.print();

        env.execute();
    }

    private static class MyAggregateFunction implements AggregateFunction<Tuple2<String, Integer>, Integer, Integer> {

        //初始化初始值
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        //每输入一条数据会调用一次该方法
        @Override
        public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
            return accumulator + value.f1; //将中间结果与当前输入的进行累加
        }

        //窗口触发后，每个key会调用一次该方法
        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        //只有Session Window会调用merge方法，如果不是SessionWindowkey不调用该方法
        @Override
        public Integer merge(Integer a, Integer b) {
            return null;
        }
    }

    private static class MyAggWindowFunction extends ProcessWindowFunction<Integer, Tuple2<String, Integer>, String, TimeWindow> {

        private transient ValueState<Integer> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("count-state", Integer.class);
            countState = getRuntimeContext().getState(stateDescriptor);
        }


        @Override
        public void process(String key, Context context, Iterable<Integer> elements, Collector<Tuple2<String, Integer>> out) throws Exception {

            Integer windowCount = elements.iterator().next();
            Integer history = countState.value();
            if (history == null) {
                history = 0;
            }
            history += windowCount;
            countState.update(history);

            out.collect(Tuple2.of(key, history));

        }
    }

}
