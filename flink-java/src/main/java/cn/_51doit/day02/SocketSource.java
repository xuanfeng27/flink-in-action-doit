package cn._51doit.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 1.说程序的并行度，决定了分布式程序执行的效率，但是也要考虑资源
 * 2.通过env的socketTextStream方法创建的DataStream的并行度是为1（只能为1，不能是多个）
 *      socketTextStream是一个非并行的Source
 * 3.调用完map方法的到的DataStream并行的为多个并行，如果没有指定，那么并行度与执行环境Env保持一致
 */
public class SocketSource {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        int parallelism = env.getParallelism();

        System.out.println("执行环境的并行度：" + parallelism);

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        int parallelism1 = lines.getParallelism();

        System.out.println("socketTextStream创建的DataStreamSource的并行度：" + parallelism1);

        SingleOutputStreamOperator<String> uppered = lines.map(line -> line.toUpperCase());

        int parallelism2 = uppered.getParallelism();

        System.out.println("调用完map方法得到的DataStream的并行度：" + parallelism2);

        DataStreamSink<String> print = uppered.print();

        int parallelism3 = print.getTransformation().getParallelism();

        System.out.println("调用完print方法得到的DataStreamSink的并行度：" + parallelism3);

        env.execute();

    }
}
