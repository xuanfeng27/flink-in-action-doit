package cn._51doit.day04;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 将多个【类型一致】的数据流合并到一起，返回一个新的流
 *
 *  将两个分区不一致的DataStream合并到一起，会得到一个新的DataStream
 *
 *
 */
public class UnionDemo2 {

    public static void main(String[] args) throws Exception{

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<String> lines1 = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Integer> nums1 = lines1.map(Integer::parseInt).setParallelism(2);


        DataStreamSource<String> lines2 = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Integer> nums2 = lines2.map(Integer::parseInt).setParallelism(3);

        DataStream<Integer> union = nums1.union(nums2);

        union.print();

        env.execute();




    }
}
