package cn._51doit.day08;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.util.Properties;


/**
 * 使用Redis Sink的步骤
 * 1.访问redis sink的官网页面：https://bahir.apache.org/docs/flink/current/flink-streaming-redis/
 * 2.在pom.xml中添加依赖
 *  <dependency>
 *   <groupId>org.apache.bahir</groupId>
 *   <artifactId>flink-connector-redis_2.12</artifactId>
 *   <version>1.1-SNAPSHOT</version>
 * </dependency>
 *
 * 3.创建redis sink
 *
 *
 * 将程序提交到集群中运行，并且是hdfs（hadoop3.x）作为state backend，必须将flink-shaded-hadoop-3-uber-3.1.1.7.1.1.0-565-9.0
 * 放到flink安装包的lib目录下，然后重启flink的standalone集群
 */

public class KafkaToRedisWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //开启checkpoint，那就把kafka的偏移量保存到状态中了，checkpoint时会将状态持久化到statebackend中
        env.enableCheckpointing(10000);

        //设置statebackend(设置状态存储的后端)
        env.setStateBackend(new FsStateBackend("hdfs://node-1.51doit.cn:9000/chk26"));

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "node-1.51doit.cn:9092,node-2.51doit.cn:9092,node-3.51doit.cn:9092");
        properties.setProperty("group.id", "test999");
        properties.setProperty("auto.offset.reset", "earliest"); //如果没有记录历史偏移量就从头读

        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(
                "wc",
                new SimpleStringSchema(),
                properties
        );
        //不将偏移量写入到kafka特殊的topic中
        flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(false);

        //spark hadoop flink flink
        DataStreamSource<String> lines = env.addSource(flinkKafkaConsumer);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        //keyBy和聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = wordAndOne.keyBy(t -> t.f0).sum(1);

        //指定redis的配置
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("172.16.100.103")
                .setPassword("123456")
                .setDatabase(9)
                .build();


        //将聚合后的数据，写入到Redis中
        summed.addSink(new RedisSink<Tuple2<String, Integer>>(conf, new RedisWordCountMapper()));

        env.execute();


    }

    public static class RedisWordCountMapper implements RedisMapper<Tuple2<String, Integer>> {

        //指定写入Redis的方式,指定value为hash方式的写入
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "WORD_COUNT");
        }

        //取出输入数据的key
        @Override
        public String getKeyFromData(Tuple2<String, Integer> data) {
            return data.f0;
        }
        //取出输入数据的value
        @Override
        public String getValueFromData(Tuple2<String, Integer> data) {
            return data.f1.toString();
        }
    }


}
