package cn._51doit.day09;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 将两个流，按照ProcessingTime划分窗口，并按照指定的条件进行join关联
 *
 * 如果能inner join上：
 *  1.两个流的数据必须在同一个窗口内
 *  2.要join的条件必须相同（相同类型，并且等值）
 *
 * 演示的就是inner join
 *
 * 需求：有两个流
 *  流1(订单id,分类id,商品金额):事实流
 *    o001,c10,2000
 *    o002,c10,1000
 *    c003,c11,5000
 *  流2(分类id,分类名称):维度流
 *    c10,图书
 *    c11,手机
 *
 * 想要得到的结果
 *   分类成交的金额
 *   c10,图书,2000
 *
 * 按照分类再进行keyBy聚合
 *
 */


public class ProcessingTimeTumblingWindowJoinDemo {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        //事实流
        //o001,c10,2000
        DataStreamSource<String> lines1 = env.socketTextStream("localhost", 8888);

        //维度流
        //c10,图书
        DataStreamSource<String> lines2 = env.socketTextStream("localhost", 9999);

        //按照ProcessingTime划分窗口，进行join
        SingleOutputStreamOperator<Tuple3<String, String, Double>> orderTpStream = lines1.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], Double.parseDouble(fields[2]));
            }
        });

        SingleOutputStreamOperator<Tuple2<String, String>> categoryTpStream = lines2.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[0], fields[1]);
            }
        });

        //如何将两个流join到一起（数据是分散在多台机器上的），必须将相同条件的数据通过网络传输到同一台机器的同一个分区内（在同一个subtask）
        //在相同的时间（窗口），相同的地点（同一个subtask）
        //将两个流按照join的条件进行keyBy，然后划分窗口
        //将第一个流放到一个包装类中（TaggedUnion<T1, Null>）,第一个字段有数据
        //将第二个流放到同一个包装类中（TaggedUnion<Null, T2>）,第二个字段有数据
        //将两个流Union到一起（要求数据类型必须一样），然后按照两个流的条件进行keyBy，然后划分窗口
        DataStream<Tuple4<String, String, Double, String>> joinedStream = orderTpStream.join(categoryTpStream)
                .where(t1 -> t1.f1) //第一流的条件
                .equalTo(t2 -> t2.f0) //第二个流的条件
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .apply(new JoinFunction<Tuple3<String, String, Double>, Tuple2<String, String>, Tuple4<String, String, Double, String>>() {

                    //窗口触发，并且两个流中有key相同的数据，才会调用join方法
                    @Override
                    public Tuple4<String, String, Double, String> join(Tuple3<String, String, Double> first, Tuple2<String, String> second) throws Exception {
                        return Tuple4.of(first.f0, first.f1, first.f2, second.f1);
                    }
                });

        joinedStream.print();

        env.execute();


    }
}
