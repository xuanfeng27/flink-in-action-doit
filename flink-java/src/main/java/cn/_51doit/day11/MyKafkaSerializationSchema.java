package cn._51doit.day11;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * 自定义一个KafkaSerializationSchema，即往Kafka中写入数据时，指定相应的序列化方式
 */
public class MyKafkaSerializationSchema implements KafkaSerializationSchema<String> {

    private String topic;

    private String charset;

    public MyKafkaSerializationSchema(String topic) {
        this.topic = topic;
        this.charset = "UTF-8";
    }

    public MyKafkaSerializationSchema(String topic, String charset) {
        this.topic = topic;
        this.charset = charset;
    }

    //来一条数据，将数据进行序列号
    @Override
    public ProducerRecord<byte[], byte[]> serialize(String input, @Nullable Long timestamp) {
        //将字符串转成byte array，然后将数据封装到ProducerRecord

        return new ProducerRecord<>(topic, input.getBytes(Charset.forName(charset)));
    }


}
