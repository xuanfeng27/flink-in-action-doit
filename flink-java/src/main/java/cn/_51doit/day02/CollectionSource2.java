package cn._51doit.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * 基于集合的Source，该类Source通常就是为了做测试或实验的，不用于生产环境
 *   fromElements创建的DataStream是一个有限的数据流，读取完数据就程序就退出了
 */
public class CollectionSource2 {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> numStream = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 10);

        System.out.println("ffromElements创建的DataStreamSource的并行度为：" + numStream.getParallelism());

        numStream.print();

        env.execute();



    }
}
