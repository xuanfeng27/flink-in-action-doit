package cn._51doit.day11;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * Flink从Kafka中读取数据，然后再讲数据写入到Kafka中，并且保证ExactlyOnce
 *
 * Kafka Source 支持ExactlyOnce
 * Kafka Sink   支持ExactlyOnce
 *
 * 比如：Flink的KafkaProducer会将数据发送到Kafka的broker，但是有可能没有提交事务，在Kafka中的事务隔离级别un_commited
 * 如果程序出现问题，消费者读到了未提交事务的数据，Flink重启后，再次发送该数据，并且提交事务，消费者会重复读到该数据
 *
 * 消费者只读取提交事务的数据
 *  如果使用命令行客户端消费kafka的数据，需要加上 --isolation-level read_committed
 *  如果使用程序修改费kafka的数据，需要加上 properties.setProperty("isolation.level", "read_committed");
 *
 */
public class KafkaToKafka {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //开启checkpoint，那就把kafka的偏移量保存到状态中了，checkpoint时会将状态持久化到statebackend中
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);

        env.setStateBackend(new FsStateBackend("file:///Users/start/Documents/flink-in-action/flink-java/chk"));

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "node-1.51doit.cn:9092,node-2.51doit.cn:9092,node-3.51doit.cn:9092");
        properties.setProperty("group.id", "test999");
        properties.setProperty("auto.offset.reset", "latest"); //如果没有记录历史偏移量就从头读
        //如果开启事务，需要将客户端的事务超时是小于broker的事务超时时间
        properties.setProperty("transaction.timeout.ms", "600000"); //broker默认值是为15分钟


        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(
                "wc",
                new SimpleStringSchema(),
                properties
        );
        //不将偏移量写入到kafka特殊的topic中
        flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(false);


        //spark hadoop flink flink
        DataStreamSource<String> lines = env.addSource(flinkKafkaConsumer);

        //使用SocketSource默认数据，用了模拟错误
        DataStreamSource<String> errorLines = env.socketTextStream("localhost", 8888);

        DataStream<String> unionStream = errorLines.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                if (value.startsWith("error")) {
                    throw new RuntimeException("有错误数据出现");
                }
                return value;
            }
        }).union(lines);


        //将数据进行ETL操作（清洗过滤、数据补全、数据脱敏、维度关联、类型转换）
        SingleOutputStreamOperator<String> filtered = unionStream.filter(e -> !e.startsWith("error"));

        //将数据写回Kafka（下面key实现AtLeastOnceOnce）
//        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
//                "my-topic",                  // target topic
//                new SimpleStringSchema(),    // serialization schema
//                properties                  // producer config
//        );

        String topic  = "my-topic";

        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                topic,
                new MyKafkaSerializationSchema(topic),
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );

        filtered.addSink(kafkaProducer);

        env.execute();


    }

}
