package cn._51doit.day07;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * 将同一个用户的行为保存起来
 */
public class UserEvent {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);

        //u001,view
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple2<String, String>> tpStream = lines.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[0], fields[1]);
            }
        });
        KeyedStream<Tuple2<String, String>, String> keyedStream = tpStream.keyBy(t -> t.f0);

        keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<String, List<String>>>() {

            private ListState<String> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ListStateDescriptor<String> listStateDescriptor = new ListStateDescriptor<>("event-state", String.class);
                listState = getRuntimeContext().getListState(listStateDescriptor);
            }

            @Override
            public void processElement(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, List<String>>> out) throws Exception {

                String event = value.f1;
                listState.add(event);

                //输入
                ArrayList<String> events = (ArrayList<String>) listState.get();
                out.collect(Tuple2.of(value.f0, events));
            }
        }).print();

        env.execute();

    }

}
