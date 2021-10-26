package cn._51doit.day09;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

/**
 * 设置状态的TTL（TimeToLive）即设置状态的存活时间
 * 默认情况，状态的数据会一直保存，但是有的数据，以后就不再使用了，如果还在状态中存储，浪费更多的资源，checkpoint的数据会越来越多
 *
 * 我们也可以设置状态的存活时间，以后超时的状态，Flink会清除，这样更加节省资源
 *
 * 需求：统计最近两小时的成交金额（保留当前小时和上一个小时的数据）
 * 为了效果演示更加明显，设置状态的存活时间为1分钟
 *
 * .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) //OnCreateAndWrite(默认的)，当创建和修改该key对应的value，就会重新对TTL计时
 * .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite) //创建、修改、读取TTL都会重新计时
 * .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) //设置状态的可见性：默认的，如果超时，就不会再返回（就访问不到了）
 * .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp) //即使超时了，但是还没有被清除掉，也可以使用
 *
 */
public class ValueStateTTLDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //商品分类，商品总金额
        //家具,1000
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, Double>> tpStream = lines.map(new MapFunction<String, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[0], Double.parseDouble(fields[1]));
            }
        });

        KeyedStream<Tuple2<String, Double>, String> keyedStream = tpStream.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple3<String, String, Double>> res = keyedStream.process(new IncomeHourlySumFunction());

        res.print();

        env.execute();

    }

    //UDF（用户自定义的Function）
    //输出的数据为Tuple3<家具, 20211025-1510, 5000>
    private static class IncomeHourlySumFunction extends KeyedProcessFunction<String, Tuple2<String, Double>, Tuple3<String, String, Double>> {

        private transient MapState<String, Double> mapState;

        private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd-HHmm");

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, Double> stateDescriptor = new MapStateDescriptor<>("hourly-state", String.class, Double.class);
            //给状态秒描述器设置TTL
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.minutes(1))
                    //设置TTL的更新类型
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) //OnCreateAndWrite(默认的)，当创建和修改该key对应的value，就会重新对TTL计时
                    //.setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite) //创建、修改、读取TTL都会重新计时
                    //设置状态可见性
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) //默认的，如果超时，就不会再返回（就访问不到了）
                    //.setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp) //即使超时了，但是还没有被清除掉，也可以使用
                    .build();//为了效果，我这里设置状态最多保存1分钟
            //将TTLConfig关联到状态描述器
            stateDescriptor.enableTimeToLive(ttlConfig);
            //根据状态描述器,创建状态
            mapState = getRuntimeContext().getMapState(stateDescriptor);

        }

        @Override
        public void processElement(Tuple2<String, Double> value, Context ctx, Collector<Tuple3<String, String, Double>> out) throws Exception {
            Double current = value.f1;
            long currentTimeMillis = System.currentTimeMillis();
            //202110251510
            String timeStr = sdf.format(new Date(currentTimeMillis));
            Double history = mapState.get(timeStr);
            if (history == null) {
                history = 0.0;
            }
            current += history;
            //更新状态
            mapState.put(timeStr, current);
            //输出数据
            Iterator<Map.Entry<String, Double>> iterator = mapState.entries().iterator();
//            while (iterator.hasNext()) {
//                Map.Entry<String, Double> entry = iterator.next();
//                out.collect(Tuple3.of(value.f0, entry.getKey(), entry.getValue()));
//            }
            out.collect(Tuple3.of(value.f0, timeStr, current));
        }
    }
}
