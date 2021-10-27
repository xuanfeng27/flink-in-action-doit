package cn._51doit.day10;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 两个数据流，使用EventTime划分窗口，实现LeftOuterJoin
 *
 *
 *
 */
public class EventTimeTumblingWindowLeftOuterJoinDemo {

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

        //实现左外连接(coGroup协同分组，就是将两个流按照相同的条件进行keyBy)
        DataStream<Tuple5<Long, String, Integer, Long, String>> joiendStream = leftStreamWithWaterMark.coGroup(rightStreamWithWaterMark)
                .where(t1 -> t1.f1)
                .equalTo(t2 -> t2.f1)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new CoGroupFunction<Tuple3<Long, String, Integer>, Tuple3<Long, String, String>, Tuple5<Long, String, Integer, Long, String>>() {

                    /**
                     * 窗口触发后，只要key在任何一个流中出现，就会调用一次coGroup
                     * @param first
                     * @param second
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void coGroup(Iterable<Tuple3<Long, String, Integer>> first, Iterable<Tuple3<Long, String, String>> second, Collector<Tuple5<Long, String, Integer, Long, String>> out) throws Exception {
                        //实现左外连接
                        for (Tuple3<Long, String, Integer> left : first) {
                            boolean isJoined = false;
                            for (Tuple3<Long, String, String> right : second) {
                                isJoined = true;
                                out.collect(Tuple5.of(left.f0, left.f1, left.f2, right.f0, right.f2));
                            }
                            if (!isJoined) {
                                out.collect(Tuple5.of(left.f0, left.f1, left.f2, null, null));
                            }
                        }
                    }
                });

        joiendStream.print();

        env.execute();

    }
}
