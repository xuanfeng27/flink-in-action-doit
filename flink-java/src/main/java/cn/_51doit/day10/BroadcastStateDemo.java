package cn._51doit.day10;

import com.sun.jna.IntegerType;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 为了提高关联维度数据（字典数据）的效率，可以使用广播状态将数据广播到下游subtask的所有分区，可以就可以实现mapSideJoin
 * 这样可以高效的实现数据的关联，并且提高效率（减少数据的网络传输）
 *
 * 什么场景可以使用广播状态？
 *  1.要广播的数据相对较小
 *  2.广播的数据可以修改
 *  3.实现高效的关联
 *
 */
public class BroadcastStateDemo {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        //第一个流（维度流）
        //c01,收音机,insert
        //c10,图书,insert
        //c11,家电,insert
        //c12,电脑,insert
        //c12,台式电脑,update
        //c01,收音机,delete
        DataStreamSource<String> lines1 = env.socketTextStream("localhost", 8888);

        //将要广播的数据整理（广播之前的数据，）
        SingleOutputStreamOperator<Tuple3<String, String, String>> tpStream = lines1.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], fields[2]);
            }
        });

        //广播状态其实是一种特殊的OperatorState（不跟key绑定在一起）
        //广播出去的数据，在下游的每一个分区中，以mapState的形式存储
        //描述以后广播的数据在下游以何种方式存储
        MapStateDescriptor<String, String> stateDescriptor = new MapStateDescriptor<>("id2NameState", String.class, String.class);

        //将整理好的tpStream进行广播,关联状态描述器，描述以后广播的数据在下游以何种方式存储
        BroadcastStream<Tuple3<String, String, String>> broadcastStream = tpStream.broadcast(stateDescriptor);

        //事实数据
        //o01,c10,2000
        DataStreamSource<String> lines2 = env.socketTextStream("localhost", 9999);

        //将要广播的数据整理（广播之前的数据，）
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> orderTpStream = lines2.map(new MapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], fields[1], Integer.parseInt(fields[2]));
            }
        });

        //将事实流和维度流进行connect（ 共享状态，事实流使用广播流的状态）
        orderTpStream.connect(broadcastStream).process(new BroadcastProcessFunction<Tuple3<String, String, Integer>, Tuple3<String, String, String>, Tuple4<String, String, Integer, String>>() {

            //每来一条维度数据调用一次processElement
            @Override
            public void processElement(Tuple3<String, String, Integer> value, ReadOnlyContext ctx, Collector<Tuple4<String, String, Integer, String>> out) throws Exception {
                String cid = value.f1;
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(stateDescriptor);
                String name = broadcastState.get(cid);
                out.collect(Tuple4.of(value.f0, value.f1, value.f2, name));
            }

            @Override
            public void processBroadcastElement(Tuple3<String, String, String> value, Context ctx, Collector<Tuple4<String, String, Integer, String>> out) throws Exception {
                System.out.println("下游的Subtask : " + getRuntimeContext().getIndexOfThisSubtask() +   " 接收到了上游广播的数据！");
                String cid = value.f0;
                String name = value.f1;
                //对维度数据的操作类型
                String type = value.f2;
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(stateDescriptor);
                if ("delete".equals(type)) {
                    broadcastState.remove(cid);
                } else {
                    broadcastState.put(cid, name);
                }
            }

        }).print();

        env.execute();


    }
}
