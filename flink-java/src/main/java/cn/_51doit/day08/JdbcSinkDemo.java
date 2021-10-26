package cn._51doit.day08;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 *
 * 使用jdbc sink 将数据写入到mysql中
 * 1.打开对应的页面：https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/connectors/datastream/jdbc/
 * 2.导入依赖
 *  <dependency>
 *     <groupId>org.apache.flink</groupId>
 *     <artifactId>flink-connector-jdbc_2.12</artifactId>
 *     <version>1.13.2</version>
 *  </dependency>
 * 3.调用addSink传入jdbcSink
 *
 *
 */
public class JdbcSinkDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //uid,addCart
        //mysql数据库有3个字段
        //id（主键、自增）, 用户id，事件类型
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, String>> tpStream = lines.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[0], fields[1]);
            }
        });

        tpStream.addSink(
                JdbcSink.sink(
                        "insert into tb_events (uid, event_type) values (?, ?)",
                        new JdbcStatementBuilder<Tuple2<String, String>>() {
                            @Override
                            public void accept(PreparedStatement preparedStatement, Tuple2<String, String> tp) throws SQLException {
                                //绑定参数
                                preparedStatement.setString(1, tp.f0);
                                preparedStatement.setString(2, tp.f1);
                            }
                        },
                        //执行选项
                        JdbcExecutionOptions.builder()
                                .withBatchSize(1000) //批量写入的数据条数
                                .withBatchIntervalMs(5000) //达到多长时间写入一次
                                .withMaxRetries(100) //如果写入失败，重试次数
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://node-3.51doit.cn:3306/doit?characterEncoding=utf-8")
                                .withDriverName("com.mysql.jdbc.Driver")
                                .withUsername("root")
                                .withPassword("123456")
                                .build()
                )
        );


        env.execute();
    }


}
