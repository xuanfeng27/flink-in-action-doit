package cn._51doit.day09;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * 只有KeyedStream，使用KeyedProcessFunction才可以使用定时器
 *
 * 使用EventTime注册定时器
 *
 * OnTimer方法触发的时机 ： WaterMark >= 注册的定时器EventTime时间
 *
 */
public class EventTimeTimer {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1000,spark,1
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> streamWithWaterMark = lines.assignTimestampsAndWatermarks(WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((line, time) -> Long.parseLong(line.split(",")[0])));


        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = streamWithWaterMark.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[1], Integer.parseInt(fields[2]));
            }
        });

        //先keyby再使用定时器
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = tpStream.keyBy(t -> t.f0);

        //调用KeyedProcessFunction
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            private transient ValueState<List<Tuple2<String, Integer>>> listValueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<List<Tuple2<String, Integer>>> stateDescriptor = new ValueStateDescriptor<>("list-state", TypeInformation.of(new TypeHint<List<Tuple2<String, Integer>>>() {
                }));
                listValueState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                //在processElement中不输出数据，而是将数据攒起来，然后注册定时器
                //攒起来的数据最好放在状态中（可以容错）
                List<Tuple2<String, Integer>> lst = listValueState.value();
                if (lst == null) {
                    lst = new ArrayList<>();
                }
                lst.add(value);
                listValueState.update(lst);

                //11:29:15 下一次触发的时间11：30：00
                //11:29:18 下一次触发的时间11：30：00
                long currentWatermark = ctx.timerService().currentWatermark();
                long triggerTime = (currentWatermark - currentWatermark % 60000) + 60000;
                System.out.println(ctx.getCurrentKey() + "注册定时器的WaterMark：" +  currentWatermark + ", 定时器触发的时间：" + triggerTime);

                //如果注册了多个触发时间相同的定时器，只会触发一次（后面注册的定时器会覆盖前面的定时器）
                ctx.timerService().registerEventTimeTimer(triggerTime);

            }

            //定时器触发后会调用onTimer方法（每个key都有单独的定时器，每个key的定时器触发了都会调用一个onTimer方法）
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

                System.out.println("触发定时器的时间为：" + timestamp);

                List<Tuple2<String, Integer>> lst = listValueState.value();
                for (Tuple2<String, Integer> tp : lst) {
                    out.collect(tp);
                }
                lst.clear();

            }
        });

        res.print();

        env.execute();


    }

}
