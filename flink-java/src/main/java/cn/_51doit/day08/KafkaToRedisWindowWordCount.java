package cn._51doit.day08;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
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
 * 将数据在窗口中增量聚合，然后将每个窗口的数据再与历史数据聚合，最后写入到Redis中
 * 优点：相比一条一条的写入，在数据量比较大的情况下，增量聚合再写入效率更高，更加节省资料
 */
public class KafkaToRedisWindowWordCount {

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

        SingleOutputStreamOperator<Tuple2<String, Integer>> res = windowedStream.reduce(new MyReduceFunction(), new MyHistorySumWindowFunction());

        res.print();

        env.execute();
    }

    private static class MyReduceFunction implements ReduceFunction<Tuple2<String, Integer>> {
        //如果使用window的reduceFunction，中间聚合的结果会保存到window state中，是可以容错的
        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
            value1.f1 = value1.f1 + value2.f1;
            return value1;
        }
    }

    /**
     * 将每个窗口增量聚合的结果，再与历史数据进行聚合
     */
    private static class MyHistorySumWindowFunction extends ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {

        private transient ValueState<Integer> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("count-state", Integer.class);
            countState = getRuntimeContext().getState(stateDescriptor);
        }

        //当窗口触发后，每个key会调用一次process方法
        @Override
        public void process(String key, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
            //窗口增量聚合的结果
            Tuple2<String, Integer> tp = elements.iterator().next();
            //获取历史数据
            Integer history = countState.value();
            if (history == null) {
                history = 0;
            }
            history += tp.f1;
            //更新
            countState.update(history);
            //输出结果
            tp.f1 = history;
            out.collect(tp);

        }
    }


}
