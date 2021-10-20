package cn._51doit.day03;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 对KeyedStream进行滚动聚合（将同一个分区中，key相同的数据对应的value进行实时聚合）
 */
public class ReduceDemo {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        //spark,1
        //hadoop,3
        //spark,2
        //hadoop,5
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCount = lines.map(line -> {
            String[] fields = line.split(",");
            return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
        }).returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));
        //按照单词进行keyBy（按照key的Hash进行分区）

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndCount.keyBy(tp -> tp.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> reduced = keyedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                value1.f1 = value1.f1 + value2.f1;
                return value1;
            }
        });

        reduced.print();

        env.execute();

    }
}
