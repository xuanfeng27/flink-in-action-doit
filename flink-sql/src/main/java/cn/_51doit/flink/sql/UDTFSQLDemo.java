package cn._51doit.flink.sql;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class UDTFSQLDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 8888);

        //hello tom jerry tom
        //tableEnv.registerDataStream("t_lines", socketTextStream, "line");
        tableEnv.createTemporaryView("t_lines", socketTextStream, $("line"));

        tableEnv.createTemporaryFunction("split", new Split(" "));

        Table table = tableEnv.sqlQuery(
                "SELECT word FROM t_lines, LATERAL TABLE(split(line)) as A(word)");

        //非聚合的Table，将表转成AppendStream
        tableEnv.toAppendStream(table, Row.class).print();

        env.execute();

    }

}
