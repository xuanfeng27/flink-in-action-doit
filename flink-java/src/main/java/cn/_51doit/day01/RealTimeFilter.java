package cn._51doit.day01;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RealTimeFilter {

    public static void main(String[] args) throws Exception {

        //创建一个执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //调用env的Source方法创建抽象数据集
        DataStreamSource<String> lines = env.socketTextStream("node-1.51doit.cn", 8888);

        //调用Transformation（s）
        DataStream<String> filtered = lines.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return !value.startsWith("error");
            }
        });

        //调用Sink
        filtered.print();

        //启动
        env.execute();

    }
}
