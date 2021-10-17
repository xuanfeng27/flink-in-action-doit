package cn._51doit.day03;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 比较Max和MaxBy的区别，如果Tuple中有两个字段，Max和MaxBy效果一样
 *
 * 如果keyBy后的数据有，大于2个，效果就不一样了
 *
 */
public class MaxKeyDemo2 {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        //辽宁省,沈阳市,2000
        //山东省,济南市,2000
        //山东省,烟台市,3000
        //辽宁省,本溪市,1000
        //辽宁省,大连市,1000
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, Integer>> provinceCityAndMoney = lines.map(line -> {
            String[] fields = line.split(",");
            return Tuple3.of(fields[0], fields[1], Integer.parseInt(fields[2]));
        }).returns(TypeInformation.of(new TypeHint<Tuple3<String, String, Integer>>() {}));

        //安装省份进行KeyBy
        KeyedStream<Tuple3<String, String, Integer>, String> keyedStream = provinceCityAndMoney.keyBy(t -> t.f0);

        //keyedStream.maxBy(2).print();
        //如果要比较的字段相等，其他字段返回最新的（默认是第一次出现的）
        keyedStream.maxBy(2, false).print();

        env.execute();




    }
}
