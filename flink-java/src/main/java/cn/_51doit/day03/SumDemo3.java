package cn._51doit.day03;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SumDemo3 {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<WordCount> wordAndCount = lines.map(line -> {
            String[] fields = line.split(",");
            return new WordCount(fields[0], Integer.parseInt(fields[1]));
        });

        KeyedStream<WordCount, String> keyedStream = wordAndCount.keyBy(w -> w.getWord());

        SingleOutputStreamOperator<WordCount> sum = keyedStream.sum("count");

        sum.print();

        env.execute();




    }



}
