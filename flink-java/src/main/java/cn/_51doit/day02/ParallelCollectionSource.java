package cn._51doit.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.LongValueSequenceIterator;

/**
 * 多并行的基于集合的Source，但是也是一个有限的数据流，数据读完，程序退出
 */
public class ParallelCollectionSource {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<LongValue> numStream = env.fromParallelCollection(new LongValueSequenceIterator(1, 10), LongValue.class);

        System.out.println("fromParallelCollection创建的DataStreamSource的并行度为：" + numStream.getParallelism());

        numStream.print();

        env.execute();



    }
}
