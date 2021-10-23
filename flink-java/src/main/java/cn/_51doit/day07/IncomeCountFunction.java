package cn._51doit.day07;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class IncomeCountFunction extends KeyedProcessFunction<String, Tuple3<String, String, Double>, Tuple4<String, Double, String, Double>> {

    private MapState<String, Double> mapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        //定义状态描述器
        MapStateDescriptor<String, Double> stateDescriptor = new MapStateDescriptor<>("map-state", String.class, Double.class);
        mapState = getRuntimeContext().getMapState(stateDescriptor);
    }

    @Override
    public void processElement(Tuple3<String, String, Double> value, Context ctx, Collector<Tuple4<String, Double, String, Double>> out) throws Exception {
        String city = value.f1;
        Double money = value.f2;
        //获取历史数据
        Double historyIncome = mapState.get(city);
        if (historyIncome == null) {
            historyIncome = 0.0;
        }
        historyIncome += money;
        //更新
        mapState.put(city, historyIncome);

        Iterable<Double> iterable = mapState.values();
        Double totalIncome = 0.0;
        for (Double aDouble : iterable) {
            totalIncome += aDouble;
        }

        //输出数据
        out.collect(Tuple4.of(value.f0, totalIncome, city, historyIncome));
    }
}
