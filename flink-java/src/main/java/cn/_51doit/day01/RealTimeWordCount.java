package cn._51doit.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class RealTimeWordCount {

    public static void main(String[] args) throws Exception{

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.调用Env的Source方法创建DataStream
        //spark hadoop flink
        //hive hadoop hbase
        DataStream<String> lines = env.socketTextStream(args[0], Integer.parseInt(args[1]));

        //3.调用Transformation（s）
        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String in, Collector<String> out) throws Exception {
                String[] words = in.split(" ");
                //for循环
                for (String word : words) {
                    //使用Collector将数据输出
                    out.collect(word);
                }
            }
        });

        //4.调用Sink将数据输出或写入到其他的存储系统
        words.print();


        //5.启动
        env.execute();




    }
}
