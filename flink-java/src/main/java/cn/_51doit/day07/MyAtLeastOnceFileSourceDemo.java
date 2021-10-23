package cn._51doit.day07;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MyAtLeastOnceFileSourceDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //开启checkpoint
        env.enableCheckpointing(5000);
        env.setParallelism(1);
        //假设有一个dir目录，目录下有4个文件，分别是0.txt、1.txt、2.txt、3.txt、
        //自定义一个并行的Source，4更并行度，0号subtask读取0.txt，1号subtask读取1.txt，以此类推
        DataStreamSource<String> lines = env.addSource(new MyAtLeastOnceFileSource("/Users/start/Desktop/dir"));
        lines.print();

        env.execute();


    }
}
