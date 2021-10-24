package cn._51doit.day08;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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

    public static void main(String[] args) {

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

//        tpStream.addSink(
//                JdbcSink.sink(
//                        "insert into books (id, title, authors, year) values (?, ?, ?, ?)",
//                        (statement, book) -> {
//                            statement.setLong(1, book.id);
//                            statement.setString(2, book.title);
//                            statement.setString(3, book.authors);
//                            statement.setInt(4, book.year);
//                        },
//                        JdbcExecutionOptions.builder()
//                                .withBatchSize(1000)
//                                .withBatchIntervalMs(200)
//                                .withMaxRetries(5)
//                                .build(),
//                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                                .withUrl("jdbc:postgresql://dbhost:5432/postgresdb")
//                                .withDriverName("org.postgresql.Driver")
//                                .withUsername("someUser")
//                                .withPassword("somePassword")
//                                .build()
//                ));
//        )


    }


}
