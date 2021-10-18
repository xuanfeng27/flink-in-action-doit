package cn._51doit.day04.partitioning;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class RebalanceDemo {

    public static void main(String[] args) throws Exception {


        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> mappedStream = lines.map(new RichMapFunction<String, String>() {

            @Override
            public String map(String value) throws Exception {
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                return value + " -> " + indexOfThisSubtask;
            }
        }).setParallelism(2);

        //使用轮询方式进行分区
        DataStream<String> rebalanced = mappedStream.rebalance();

        //将数据输出
        rebalanced.addSink(new RichSinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                System.out.println(value + " -> " + indexOfThisSubtask);
            }
        }).setParallelism(3);

        env.execute();

    }


}
