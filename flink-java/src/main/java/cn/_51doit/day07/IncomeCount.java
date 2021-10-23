package cn._51doit.day07;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *
 * KeyedState是Flink中KeyedStream使用的跟key绑定的状态
 * KeyedState有3种：
 *
 * valueState
 * 	Map<key, value>
 *
 * mapState
 * 	Map<key, Map<k, v>>
 *
 * listState
 * 	Map<key, List<V>>
 *
 *
 * 需求：
 * 河北省,保定市,3000
 * 河北省,廊坊市,2000
 * 河北省,保定市,2000
 *
 * 辽宁省,沈阳市,1000
 * 辽宁省,大连市,2000
 * 辽宁省,大连市,2000
 *
 * 相同省份的数据分到同一个分区，并且按照城市统计金额
 * 按照省份进行keyBy
 *
 */
public class IncomeCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(30000);

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple3<String, String, Double>> provinceCityAndMoneyStream = lines.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], Double.parseDouble(fields[2]));
            }
        });

        KeyedStream<Tuple3<String, String, Double>, String> keyedStream = provinceCityAndMoneyStream.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple4<String, Double, String, Double>> res = keyedStream.process(new IncomeCountFunction());

        res.print();

        env.execute();


    }

}
