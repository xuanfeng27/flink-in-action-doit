package cn._51doit.day04;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 将多个【类型一致】的数据流合并到一起，返回一个新的流
 */
public class UnionDemo {

    public static void main(String[] args) throws Exception{

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<String> lines1 = env.socketTextStream("localhost", 8888);
        DataStreamSource<String> lines2 = env.socketTextStream("localhost", 9999);

        DataStream<String> union = lines1.union(lines2);

        union.print();

        env.execute();




    }
}
