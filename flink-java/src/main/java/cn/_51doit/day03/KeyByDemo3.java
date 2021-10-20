package cn._51doit.day03;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 将省份、城市联合起来进行keyBy
 */
public class KeyByDemo3 {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        //辽宁省,沈阳市,2000
        //山东省,济南市,2000
        //山东省,烟台市,3000
        //辽宁省,本溪市,1000
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, Integer>> provinceCityAndMoney = lines.map(line -> {
            String[] fields = line.split(",");
            return Tuple3.of(fields[0], fields[1], Integer.parseInt(fields[2]));
        }).returns(TypeInformation.of(new TypeHint<Tuple3<String, String, Integer>>() {}));

        //按照省份进行key
        //KeyedStream<Tuple3<String, String, Integer>, String> keyedStream = provinceCityAndMoney.keyBy(tp -> tp.f0);
        //如果要keyBy的是为Tuple类型，可以使用下标，下标的编号从0开始

//        KeyedStream<Tuple3<String, String, Integer>, Tuple2<String, String>> keyedStream = provinceCityAndMoney.keyBy(new KeySelector<Tuple3<String, String, Integer>, Tuple2<String, String>>() {
//            @Override
//            public Tuple2<String, String> getKey(Tuple3<String, String, Integer> tp3) throws Exception {
//                return Tuple2.of(tp3.f0, tp3.f1);
//            }
//        });

        //如果keyBy的条件被封装到tuple中，并且使用了lambda表达式，必须在keyBy的第二个参数指定类型
        //KeyedStream<Tuple3<String, String, Integer>, Tuple2<String, String>> keyedStream = provinceCityAndMoney.keyBy(tp3 -> Tuple2.of(tp3.f0, tp3.f1), TypeInformation.of(new TypeHint<Tuple2<String, String>>() {}));

        KeyedStream<Tuple3<String, String, Integer>, String> keyedStream = provinceCityAndMoney.keyBy(t -> t.f0 + t.f1);

        keyedStream.print();


        env.execute();




    }
}
