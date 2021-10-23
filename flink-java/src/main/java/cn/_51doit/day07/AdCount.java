package cn._51doit.day07;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 度统计广告曝光、点击的次数和人数
 */
public class AdCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);

        //a001,u1,view
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //将数据整理
        SingleOutputStreamOperator<Tuple3<String, String, String>> tpStream = lines.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], fields[2]);
            }
        });

        //对数据进行keyBy
        //广告ID和事件ID
        KeyedStream<Tuple3<String, String, String>, Tuple2<String, String>> keyedStream = tpStream.keyBy(new KeySelector<Tuple3<String, String, String>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Tuple3<String, String, String> value) throws Exception {
                return Tuple2.of(value.f0, value.f2);
            }
        });

        SingleOutputStreamOperator<Tuple4<String, String, Integer, Integer>> res = keyedStream.map(new AdCountFunction());

        res.print();

        env.execute("adCount");


    }
}
