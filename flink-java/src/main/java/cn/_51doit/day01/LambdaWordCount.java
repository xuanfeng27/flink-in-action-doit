package cn._51doit.day01;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class LambdaWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> wordsDataStream = lines.flatMap((String line, Collector<String> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(word);
            }
        }).returns(Types.STRING); //使用Lambda表达式，如果返回的类型有泛型，必须指定类型

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = wordsDataStream.map(w -> Tuple2.of(w, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndOne.keyBy(tp -> tp.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = keyed.sum(1);

        summed.print();

        env.execute();



    }
}
