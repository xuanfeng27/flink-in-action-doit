package cn._51doit.day07;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MyAtLeastOnceFileSourceDemo {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        //开启checkpoint
        env.enableCheckpointing(20000);
        env.setParallelism(4);
        //假设有一个dir目录，目录下有4个文件，分别是0.txt、1.txt、2.txt、3.txt、
        //自定义一个并行的Source，4更并行度，0号subtask读取0.txt，1号subtask读取1.txt，以此类推
        DataStreamSource<String> lines = env.addSource(new MyAtLeastOnceFileSource("/Users/start/Desktop/dir"));


        DataStreamSource<String> lines2 = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<String> errorLines = lines2.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                if (value.startsWith("error")) {
                    throw new RuntimeException("有错误数据！");
                }
                return value;
            }
        });

        lines.union(errorLines).print();


        env.execute();


    }
}
