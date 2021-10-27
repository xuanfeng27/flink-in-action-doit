package cn._51doit.day10;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * 使用异步IO请求高德地图根据经纬度返回省市去街道、商圈
 *
 * 为什么要使用异步IO
 *  1.如果要关联的数据非常大，比如用户的画像数据，非常大，存储在Hbase中
 *  2.你要广播的数据压根没有，必须访问别的公司的接口、数据库
 *  3.异步IO比传统的同步查询（串行）效率高，要效果稍微多一些资源
 *
 */
public class AsyncIODemo1 {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //测试数据
        //{"oid": "o1000", "cid": "c10", "money": 99.99, "longitude": 116.413467, "latitude": 39.908072}
        //{"oid": "o1001", "cid": "c11", "money": 99.99, "longitude": 116.413467, "latitude": 39.908072}
        //{"oid": "o1000", "cid": "c10", "money": 99.99, "longitude": 116.413467, "latitude": 39.908072}
        //{"oid": "o1001", "cid": "c11", "money": 99.99, "longitude": 116.413467, "latitude": 39.908072}
        //{"oid": "o1000", "cid": "c10", "money": 99.99, "longitude": 116.413467, "latitude": 39.908072}
        //{"oid": "o1001", "cid": "c11", "money": 99.99, "longitude": 116.413467, "latitude": 39.908072}
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //使用异步IO进行查询关联信息
        //unorderedWait发送请求返回的结果与发送请求的先后顺序可以一致
        //orderedWait发送请求返回的结果与发送请求的先后顺序一致
        SingleOutputStreamOperator<OrderBean> result = AsyncDataStream.unorderedWait(lines, new AsyncHttpQueryFunction(), 2000, TimeUnit.MILLISECONDS, 10);

        result.print();

        env.execute();




    }
}
