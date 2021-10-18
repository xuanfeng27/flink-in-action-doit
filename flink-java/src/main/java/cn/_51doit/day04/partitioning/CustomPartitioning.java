package cn._51doit.day04.partitioning;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * 自定义分区器，可以根据想要的分区规则进行分区
 */
public class CustomPartitioning {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, String>> tpStream = lines.map(new RichMapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                return Tuple2.of(value, indexOfThisSubtask + "");
            }
        }).setParallelism(2);

        DataStream<Tuple2<String, String>> stream2 = tpStream.partitionCustom(
                new Partitioner<String>() {

                    /**
                     *
                     * @param key 通过KeySelector获取到分区的key
                     * @param numPartitions 下游分区的数量
                     * @return
                     */
                    @Override
                    public int partition(String key, int numPartitions) {
                        System.out.println("下游分区的数量为：" + numPartitions);
                        int index = 0;
                        if (key.startsWith("h")) {
                            index = 1;
                        } else if (key.startsWith("f")) {
                            index = 2;
                        } else if (key.startsWith("s")) {
                            index = 3;
                        }
                        return index;
                    }
                },
                t -> t.f0 //将tuple中的f0当成key
        );

        stream2.addSink(new RichSinkFunction<Tuple2<String, String>>() {
            @Override
            public void invoke(Tuple2<String, String> value, Context context) throws Exception {
                //bbb : -> 0 -> 2
                System.out.println(value.f0 + " : " + value.f1 + " -> " + getRuntimeContext().getIndexOfThisSubtask());
            }
        });


        env.execute();

    }
}
