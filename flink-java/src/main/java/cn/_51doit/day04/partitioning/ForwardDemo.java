package cn._51doit.day04.partitioning;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * forward ： 直传（上下游并行度保持一致，相同分区编号的subtask，上游的直传下游的subtask）
 */
public class ForwardDemo {

    public static void main(String[] args) throws Exception {


        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> mappedStream = lines.map(new RichMapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                return value + " MAP: -> " + indexOfThisSubtask;
            }
        });

        //直传
        //DataStream<String> rebalanced = mappedStream.forward();

        //将数据输出
        mappedStream.addSink(new RichSinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                System.out.println(value + " SINK: -> " + indexOfThisSubtask);
            }
        });

        env.execute();

    }


}
