package cn._51doit.flink.sql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class SQLKafkaTableJSON {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(
                "CREATE TABLE KafkaTable2 (\n" +
                        "  `ts` TIMESTAMP(3) METADATA FROM 'timestamp',\n" +
                        "  `user_id` BIGINT,\n" +
                        "  `item_id` BIGINT,\n" +
                        "  `behavior` STRING\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'kafka-json',\n" + //指定读取数据对应的topic
                        "  'properties.bootstrap.servers' = 'node-1.51doit.cn:9092,node-2.51doit.cn:9092,node-3.51doit.cn:9092',\n" +
                        "  'properties.group.id' = 'testGroup2',\n" +
                        "  'scan.startup.mode' = 'earliest-offset',\n" +
                        "  'format' = 'json',\n" +
                        "  'json.ignore-parse-errors' = 'true'\n" +
                        ")"
        );

        Table table = tEnv.sqlQuery("SELECT * FROM KafkaTable2");

        DataStream<Row> appendStream = tEnv.toAppendStream(table, Row.class);

        appendStream.print();

        env.execute();


    }
}
