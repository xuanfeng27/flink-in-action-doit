package cn._51doit.flink.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class UDAFSQLDemo {

    public static void main(String[] args) throws Exception{

        //DataStream 首先要创建 StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000));
        env.enableCheckpointing(10000);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //spark,4
        //hive,3
        //spark,2
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                if(value.startsWith("error")) {
                    throw new RuntimeException("出现异常了");
                }
                String[] fields = value.split(",");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        });

        tableEnv.createTemporaryFunction("mySum", MySumAggFunction.class);

        //注册成（视图）
        tableEnv.createTemporaryView("tb_wordcount", tpStream, $("word"), $("counts"));

        //写SQL
        Table table = tableEnv.sqlQuery("select word, mySum(counts) total_counts from tb_wordcount group by word");

        //将Table转成一个可以变化的数据流
        DataStream<Tuple2<Boolean, Row>> res = tableEnv.toRetractStream(table, Row.class);

        res.print();

        env.execute();




    }
}
