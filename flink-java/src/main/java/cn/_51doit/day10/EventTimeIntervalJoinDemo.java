package cn._51doit.day10;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 两个数据流，使用EventTime划分窗口，按照时间范围进行join
 * Interval join是将两个流Connect到一起，再按照相同条件进行keyBy，然后将各自的数据缓存到共享的状态中
 * 并且设置共享状态（KeyedState）的TTL，如果在一定的时间范围内能够找到相同条件的数据，那么join上了
 *
 *
 */
public class EventTimeIntervalJoinDemo {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        //使用老的API
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //1000,c10,2000
        DataStreamSource<String> leftLines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> leftTpStream = leftLines.map(new MapFunction<String, Tuple3<Long, String, Integer>>() {
            @Override
            public Tuple3<Long, String, Integer> map(String line) throws Exception {
                String[] fields = line.split(",");
                long ts = Long.parseLong(fields[0]);
                String cid = fields[1];
                int money = Integer.parseInt(fields[2]);
                return Tuple3.of(ts, cid, money);
            }
        }).setParallelism(2);

        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> leftStreamWithWaterMark = leftTpStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Long, String, Integer>>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(Tuple3<Long, String, Integer> element) {
                return element.f0;
            }
        });


        //1000,c10,图书
        DataStreamSource<String> rightLines = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple3<Long, String, String>> rightTpStream = rightLines.map(new MapFunction<String, Tuple3<Long, String, String>>() {
            @Override
            public Tuple3<Long, String, String> map(String line) throws Exception {
                String[] fields = line.split(",");
                long ts = Long.parseLong(fields[0]);
                String cid = fields[1];
                String name = fields[2];
                return Tuple3.of(ts, cid, name);
            }
        }).setParallelism(2);

        SingleOutputStreamOperator<Tuple3<Long, String, String>> rightStreamWithWaterMark = rightTpStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Long, String, String>>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(Tuple3<Long, String, String> element) {
                return element.f0;
            }
        });

        KeyedStream<Tuple3<Long, String, Integer>, String> leftKeyedStream = leftStreamWithWaterMark.keyBy(t -> t.f1);

        KeyedStream<Tuple3<Long, String, String>, String> rightKeyedStream = rightStreamWithWaterMark.keyBy(t -> t.f1);

        SingleOutputStreamOperator<Tuple5<Long, String, Integer, Long, String>> res = leftKeyedStream.intervalJoin(rightKeyedStream)
                .between(Time.seconds(-1), Time.seconds(1)) //以当前时间为时间标准，向前1秒，向后1秒，可以join上
                .lowerBoundExclusive() //前开后闭区间(,]
                .process(new ProcessJoinFunction<Tuple3<Long, String, Integer>, Tuple3<Long, String, String>, Tuple5<Long, String, Integer, Long, String>>() {
                    //当条件相同的数据，并且在一定的时间范围内
                    @Override
                    public void processElement(Tuple3<Long, String, Integer> left, Tuple3<Long, String, String> right, Context ctx, Collector<Tuple5<Long, String, Integer, Long, String>> out) throws Exception {
                        out.collect(Tuple5.of(left.f0, left.f1, left.f2, right.f0, right.f2));
                    }
                });

        res.print();

        env.execute();

    }
}
