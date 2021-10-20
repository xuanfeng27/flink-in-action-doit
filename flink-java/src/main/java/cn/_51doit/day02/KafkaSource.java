package cn._51doit.day02;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 使用KafkaSource从Kafka消息队列中读取数据
 *   1.KafkaSource创建的DataStream是一个并行的DataStream
 *   2.KafkaSource创建的DataStream是一个无限的数据流
 *
 * 使用步骤：
 *  https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/connectors/datastream/kafka/
 *
 *  1.导入依赖：
 *      <dependency>
 *        <groupId>org.apache.flink</groupId>
 *        <artifactId>flink-connector-kafka_2.11</artifactId>
 *        <version>1.13.2</version>
 *      </dependency>
 *  
 *  2.new FlinkKafkaConsumer
 *
 *  3.调用env的addSource传入FlinkKafkaConsumer实例
 *
 *
 *
 */
public class KafkaSource {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        //设置Evn的并行度
        env.setParallelism(2);

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "node-1.51doit.cn:9092,node-2.51doit.cn:9092,node-3.51doit.cn:9092");
        properties.setProperty("group.id", "test777");
        properties.setProperty("auto.offset.reset", "earliest"); //如果没有记录历史偏移量就从头读


        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(
                "worldcount",
                new SimpleStringSchema(),
                properties
        );

        //调用env的addSource方法创建DataStream
        DataStreamSource<String> lines = env.addSource(flinkKafkaConsumer);

        System.out.println("kafkaSource的并行度：" + lines.getParallelism());

        lines.print();

        env.execute();


    }
}
