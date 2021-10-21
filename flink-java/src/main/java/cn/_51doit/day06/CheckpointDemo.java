package cn._51doit.day06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * checkpoint 是 Flink程序的容错机制，如果开启checkpoint，默认重启策略为无限重启
 *
 * checkpointing是周期性执行的，如果程序出现异常，要重启该job对应的所有subtask
 *
 * subtask重启，先从最近异常成功的checkpoint目录恢复数据，然后接着计算
 *
 *
 */
public class CheckpointDemo {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        //如果开启了checkpoint，那么job的默认重启策略为无限重启（Integer.MAX_VALUE）
        //env.enableCheckpointing(10000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000));

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

        tpStream.keyBy(t -> t.f0).sum(1).print();

        env.execute();

    }
}
