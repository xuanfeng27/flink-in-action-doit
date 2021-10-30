package cn._51doit.flink.sql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class SQLKafkaTable {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //u001,i1000,view
        //创建一个Source表
        tEnv.executeSql(
                "CREATE TABLE KafkaTable (\n" +
                        "  `user_id` STRING,\n" +
                        "  `item_id` STRING,\n" +
                        "  `behavior` STRING,\n" +
                        "  `ts` TIMESTAMP(3) METADATA FROM 'timestamp'\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'kafka-csv',\n" + //指定读取kafka的topic
                        "  'properties.bootstrap.servers' = 'node-1.51doit.cn:9092,node-2.51doit.cn:9092,node-3.51doit.cn:9092',\n" +
                        "  'properties.group.id' = 'testGroup',\n" +
                        "  'scan.startup.mode' = 'earliest-offset',\n" +
                        "  'format' = 'csv',\n" +
                        "  'csv.ignore-parse-errors' = 'true'\n" +
                        ")"
        );

        Table table = tEnv.sqlQuery("SELECT * FROM KafkaTable where ts >= 1635585660000");

        DataStream<Row> appendStream = tEnv.toAppendStream(table, Row.class);

        appendStream.print();

        env.execute();


    }
}
