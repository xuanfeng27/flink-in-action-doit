package cn._51doit.flink.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 从Kafka中读数据，然后关联MySQL的维表，最后将结果输出到MySQL中
 * 如果使用DataStream API：KafkaSource -> Map 查询MySQL关联维表（MapState做缓存） -> JDBCSink 关联后的数据写入到MySQL
 */
public class MysqlDimDemo2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //创建一个Source表（以后从哪里读取数据，从Kafka读取数据）
        //测试数据如下
        //101,201,1008010,addCart
        tEnv.executeSql(
                "CREATE TABLE kafka_item (\n" +
                        "  `user_id` INT,\n" +
                        "  `item_id` INT,\n" +
                        "  `category_id` INT,\n" +
                        "  `action` STRING,\n" +
                        "  `ts` TIMESTAMP(3) METADATA FROM 'timestamp',\n" +
                        "  proctime as PROCTIME(), --通过计算列产生一个处理时间列\n" +
                        "  eventTime as ts, -- 事件时间\n" +
                        "  WATERMARK FOR eventTime as eventTime - INTERVAL '5' SECOND  -- 在eventTime上定义watermark\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'kafka-csv',\n" +
                        "  'properties.bootstrap.servers' = 'node-1.51doit.cn:9092,node-2.51doit.cn:9092,node-3.51doit.cn:9092',\n" +
                        "  'properties.group.id' = 'test456',\n" +
                        "  'scan.startup.mode' = 'latest-offset',\n" +
                        "  'csv.ignore-parse-errors' = 'true', \n" +
                        "  'format' = 'csv'\n" +
                        ")"
        );


        //指定维度表
        tEnv.executeSql(
                "CREATE TABLE mysql_category_dim (\n" +
                        "    id INT,\n" +
                        "    name VARCHAR\n" +
                        ") WITH (\n" +
                        "    'connector' = 'jdbc',\n" +
                        "    'url' = 'jdbc:mysql://node-3.51doit.cn:3306/aaa?characterEncoding=utf-8',\n" +
                        "    'table-name' = 'tb_category',\n" +
                        "    'driver' = 'com.mysql.jdbc.Driver',\n" +
                        "    'username' = 'root',\n" +
                        "    'password' = '123456',\n" +
                        "    'lookup.cache.max-rows' = '5000',\n" +
                        "    'lookup.cache.ttl' = '10min'\n" +
                        ")"

        );
        //创建视图，将Source表的数据查出来，join维表, 然后得到一个中间表
        tEnv.executeSql(
                "CREATE VIEW v_item_categroy AS\n" +
                        "SELECT\n" +
                        "  i.user_id, \n" +
                        "  i.item_id, \n" +
                        "  i.category_id, \n" +
                        "  i.action, \n" +
                        "  p.name \n" +
                        "FROM kafka_item AS i LEFT JOIN mysql_category_dim FOR SYSTEM_TIME AS OF i.proctime AS p \n" +
                        "ON i.category_id = p.id"
        );

        //创建一个Sink表
        tEnv.executeSql(
                "CREATE TABLE result_sink (\n" +
                        "    user_id INT, \n" +
                        "    item_id INT, \n" +
                        "    category_id INT, \n" +
                        "    action VARCHAR, \n" +
                        "    name VARCHAR \n" +
                        ") WITH (\n" +
                        "    'connector' = 'jdbc',\n" +
                        "    'url' = 'jdbc:mysql://node-3.51doit.cn:3306/aaa?characterEncoding=utf-8',\n" +
                        "    'username' = 'root',\n" +
                        "    'password' = '123456',\n" +
                        "    'table-name' = 'tb_out2' \n" +
                        ")"
        );

        //从视图中取数据插入到Sink表中
        TableResult tableResult = tEnv.executeSql(
                "INSERT INTO result_sink \n" +
                        "SELECT \n" +
                        "  user_id, item_id, category_id, action, name \n" +
                        "FROM v_item_categroy"
        );

        tableResult.print();
        //System.out.println(tableResult.getJobClient().get().getJobStatus());

//        Table t = tEnv.sqlQuery(
//                "SELECT \n" +
//                        "  user_id, item_id, category_id, action, name \n" +
//                        "FROM v_item_categroy"
//        );
//
//        //DataStream<Row> res = tEnv.toAppendStream(t, Row.class);
////
//        //res.print();


        env.execute();

    }
}
