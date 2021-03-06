package cn._51doit.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * 基于集合的Source，该类Source通常就是为了做测试或实验的，不用于生产环境
 */
public class CollectionSource {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Integer> nums = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        DataStreamSource<Integer> numStream = env.fromCollection(nums);

        System.out.println("fromCollection创建的DataStreamSource的并行度为：" + numStream.getParallelism());

        numStream.print();

        env.execute();



    }
}
