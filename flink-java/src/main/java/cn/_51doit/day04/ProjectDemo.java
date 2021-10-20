package cn._51doit.day04;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * project 投影，只针对于Tuple类的DataStream，可以实现project取出想要的部分字段
 */
public class ProjectDemo {

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


        SingleOutputStreamOperator<Tuple> resStream = provinceCityAndMoney.project(2, 1);

        resStream.print();

        env.execute();




    }
}
