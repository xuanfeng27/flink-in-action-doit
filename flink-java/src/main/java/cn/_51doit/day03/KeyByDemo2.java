package cn._51doit.day03;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyByDemo2 {

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
        //.returns(Types.TUPLE(Types.STRING, Types.INT));

        //new KeySelector<Tuple2<String, Integer>, String>
        //第一个泛型为输入的数据类型
        //第二个泛型为分区的条件（按照哪个字段进行hash）
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndCount.keyBy(tp -> tp.f0);

        keyedStream.print();

        env.execute();




    }
}
