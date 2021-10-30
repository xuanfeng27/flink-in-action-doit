package cn._51doit.flink.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class StreamTableWordCount {

    public static void main(String[] args) throws Exception {

        //DataStream 首先要创建 StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //spark,4
        //hive,3
        //spark,2
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        });

        //Table table = tableEnv.fromDataStream(tpStream, "word,counts");

        //从一个数据量，创建一个Table
        Table table = tableEnv.fromDataStream(tpStream, $("word"), $("counts"));

        //调用Table API（DSL）
        Table res = table.groupBy($("word"))
                .select($("word"), $("counts").sum().as("total-counts"));

        DataStream<Tuple2<Boolean, Tuple2<String, Integer>>> tuple2DataStream = tableEnv.toRetractStream(res, TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
        }));

        //tuple2DataStream.filter(t -> t.f0)

        tuple2DataStream.print();

        env.execute();

        //tableEnv.execute("StreamSQLWordCount");

    }
}
