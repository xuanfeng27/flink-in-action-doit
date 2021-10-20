package cn._51doit.day02;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 使用local模式执行Flink程序，并且开启Flink的webUI
 *  1.在pom.xml中引入依赖
 *      <dependency>
*             <groupId>org.apache.flink</groupId>
*             <artifactId>flink-runtime-web_${scala.binary.version}</artifactId>
*             <version>${flink.version}</version>
*         </dependency>
 *  2.调用StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
 *  3.如果使用的是createLocalEnvironmentWithWebUI(configuration)，那么提交到集群中执行，必须改成.getExecutionEnvironment();
 *
 *
 */
public class LocalWebUI {

    public static void main(String[] args) throws Exception{

        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Configuration configuration = new Configuration();
        //创建一个带webUI的本地执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

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
