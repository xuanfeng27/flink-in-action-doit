package cn._51doit.day07;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;

import java.util.HashSet;

/**
 * 使用状态
 */
public class AdCountFunction extends RichMapFunction<Tuple3<String, String, String>, Tuple4<String, String, Integer, Integer>> {

    private ValueState<Integer> countState;
    private ValueState<HashSet<String>> uidState;

    @Override
    public void open(Configuration parameters) throws Exception {
        //定义一个状态描述器
        //统计次数的State
        ValueStateDescriptor<Integer> countStateDescriptor = new ValueStateDescriptor<>("count-state", Integer.class);
        countState = getRuntimeContext().getState(countStateDescriptor);
        //记录用户ID的State
        ValueStateDescriptor<HashSet<String>> uidStateDescriptor = new ValueStateDescriptor<>("distinct-uid-state", TypeInformation.of(new TypeHint<HashSet<String>>() {}));
        uidState = getRuntimeContext().getState(uidStateDescriptor);
    }

    @Override
    public Tuple4<String, String, Integer, Integer> map(Tuple3<String, String, String> in) throws Exception {
        String aid = in.f0;
        String uid = in.f1;
        String eventType = in.f2;

        //统计次数
        Integer historyCount = countState.value();
        if (historyCount == null) {
            historyCount = 0;
        }
        historyCount += 1;
        //更新状态
        countState.update(historyCount);

        //人数
        HashSet<String> uidSet = uidState.value();
        if (uidSet == null) {
            uidSet = new HashSet<>();
        }
        //将用户ID保存到uidSet
        uidSet.add(uid);
        //更新
        uidState.update(uidSet);

        return Tuple4.of(aid, eventType, historyCount, uidSet.size());
    }
}
